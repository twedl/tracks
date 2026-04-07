"""Generate parking / dock maneuver geometry for tractor-trailer.

Five maneuver types modeled after real CDL backing patterns:

  1. STRAIGHT_BACK   — Dock with straight approach. Truck reverses in a line.
  2. ALLEY_DOCK      — 45° setup, reverse arc into perpendicular dock. Most common at DCs.
  3. BLIND_SIDE      — Like alley dock but dock is on passenger side. Slower, more GOAL stops.
  4. PULL_THROUGH    — No backing. Wide arc into a pull-through lane. Truck stops / fuel islands.
  5. ANGLE_BACK      — Back into an angled parking space (45-60°). Truck stop parking rows.

Key dimensions (standard 53' trailer):
  - Tractor wheelbase:          ~6.1 m  (20 ft)
  - Kingpin to rear trailer axle: ~12.2 m (40 ft)
  - Min steer-axle turning radius: ~12.8 m (42 ft)
  - Swept path width in a turn:  ~4.5 m
  - Overall length:              ~22 m   (72 ft)

GPS unit is on the cab roof, so heading = cab heading, not trailer heading.
During a jackknife-style reverse the cab heading diverges from the direction
of travel — this is the key signature that distinguishes backing from driving.

All geometry is built in a local meter frame (X=east, Y=north) centered
on the target point, then projected to lat/lon.
"""

import enum
import math
from datetime import datetime, timedelta

import numpy as np

from .models import TracePoint
from .interpolator import bearing as compute_bearing

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_TURN_RADIUS = 12.8   # meters, steer axle
TRAILER_KING_TO_AXLE = 12.2  # meters
MPH_TO_MPS = 0.44704


class ManeuverType(enum.Enum):
    STRAIGHT_BACK = "straight_back"
    ALLEY_DOCK = "alley_dock"
    BLIND_SIDE = "blind_side"
    PULL_THROUGH = "pull_through"
    ANGLE_BACK = "angle_back"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _heading_to_xy(heading_deg: float) -> tuple[float, float]:
    """Convert compass heading (0=N, clockwise) to unit vector (dx_east, dy_north)."""
    rad = math.radians(heading_deg)
    return math.sin(rad), math.cos(rad)


def _offset_to_latlon(
    base_lat: float, base_lon: float, dx_east: float, dy_north: float
) -> tuple[float, float]:
    dlat = dy_north / 111320.0
    dlon = dx_east / (111320.0 * math.cos(math.radians(base_lat)))
    return base_lat + dlat, base_lon + dlon


def _arc_points(
    center: tuple[float, float],
    radius: float,
    start_angle: float,
    end_angle: float,
    num_points: int = 15,
) -> list[tuple[float, float]]:
    """Generate points along a circular arc in local XY.

    Angles are in degrees, measured counter-clockwise from east (math convention).
    """
    angles = np.linspace(math.radians(start_angle), math.radians(end_angle), num_points)
    cx, cy = center
    return [(cx + radius * math.cos(a), cy + radius * math.sin(a)) for a in angles]


def _straight_points(
    start: tuple[float, float],
    heading_deg: float,
    length: float,
    num_points: int = 5,
) -> list[tuple[float, float]]:
    """Generate evenly spaced points along a straight line."""
    hx, hy = _heading_to_xy(heading_deg)
    return [
        (start[0] + hx * length * i / (num_points - 1),
         start[1] + hy * length * i / (num_points - 1))
        for i in range(num_points)
    ]


def _heading_between(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    """Compass heading from p1 to p2 in local XY (0=N, clockwise)."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def _compass_to_math(heading_deg: float) -> float:
    """Convert compass heading (0=N CW) to math angle (0=E CCW)."""
    return (90 - heading_deg) % 360


def _math_to_compass(angle_deg: float) -> float:
    """Convert math angle (0=E CCW) to compass heading (0=N CW)."""
    return (90 - angle_deg) % 360


def _build_trace(
    path_xy: list[tuple[float, float]],
    speeds_mph: list[float],
    cab_headings: list[float],
    base_lat: float,
    base_lon: float,
    start_time: datetime,
    rng,
    heading_jitter: float = 0.0,
) -> list[TracePoint]:
    """Convert a local-XY path + speed/heading arrays into TracePoints."""
    points: list[TracePoint] = []
    current_time = start_time

    for i in range(len(path_xy)):
        x, y = path_xy[i]
        lat, lon = _offset_to_latlon(base_lat, base_lon, x, y)
        speed = speeds_mph[i]
        hdg = cab_headings[i]

        if heading_jitter > 0:
            hdg = (hdg + rng.uniform(-heading_jitter, heading_jitter)) % 360

        points.append(TracePoint(
            lat=round(lat, 6),
            lon=round(lon, 6),
            speed_mph=round(speed, 1),
            heading=round(hdg, 1),
            timestamp=current_time,
        ))

        # Advance time to next point
        if i < len(path_xy) - 1:
            dist = math.hypot(
                path_xy[i + 1][0] - x,
                path_xy[i + 1][1] - y,
            )
            if speed < 0.1:
                # Stopped — GOAL stop or dwell
                dt = rng.uniform(8, 20)
            else:
                dt = dist / max(speed * MPH_TO_MPS, 0.01)
            current_time += timedelta(seconds=max(dt, 2))

    return points


# ---------------------------------------------------------------------------
# Maneuver generators — each returns (path_xy, speeds_mph, cab_headings)
# ---------------------------------------------------------------------------

def _straight_back(
    target_heading: float,
    rng,
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """Straight-line reverse into a dock directly behind the approach line.

    Geometry:
      - Approach 100m along target_heading toward the dock
      - Stop, shift to reverse
      - Back straight 30m to the dock at 1-2 mph
      - Minor steering corrections ±1.5° (driver keeping it straight)
    """
    hx, hy = _heading_to_xy(target_heading)
    reverse_heading = (target_heading + 180) % 360  # cab faces away from dock

    # Approach corridor: 100m out, driving toward dock
    approach_start = (100 * hx, 100 * hy)
    approach_end = (30 * hx, 30 * hy)  # stop 30m from dock

    approach_pts = _straight_points(approach_start, (target_heading + 180) % 360, 70, num_points=5)
    approach_speeds = np.linspace(12.0, 4.0, 5).tolist()
    approach_headings = [target_heading] * 5

    # Setup stop
    stop_pt = [approach_end]
    stop_speed = [0.0]
    stop_heading = [target_heading]

    # Reverse: 30m straight back to origin (dock)
    reverse_pts = _straight_points(approach_end, target_heading, 30, num_points=12)
    # Flip order — we're backing, so path goes from 30m out toward (0,0)
    reverse_pts = list(reversed(reverse_pts))
    # But we build path in time order: first point is where we start reversing (30m out)
    reverse_pts = _straight_points(approach_end, (target_heading + 180) % 360, 30, num_points=12)

    reverse_speeds = []
    for i in range(12):
        if i in (3, 7):
            reverse_speeds.append(0.0)  # GOAL stops
        elif i >= 10:
            reverse_speeds.append(0.8)  # final creep
        else:
            reverse_speeds.append(rng.uniform(1.5, 2.5))

    # Cab heading during straight reverse: facing away from dock + minor corrections
    reverse_headings = [reverse_heading + rng.uniform(-1.5, 1.5) for _ in range(12)]

    path = approach_pts + stop_pt + reverse_pts
    speeds = approach_speeds + stop_speed + reverse_speeds
    headings = approach_headings + stop_heading + reverse_headings

    return path, speeds, headings


def _alley_dock(
    target_heading: float,
    rng,
    approach_side: str = "right",
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """45-degree alley dock into a perpendicular loading dock.

    The most common maneuver at warehouses and distribution centers.

    Geometry:
      1. Approach on yard road past the dock
      2. Pull forward + angle away to set up (cab turns 30-45° from dock line)
      3. Reverse while steering trailer toward dock — circular arc
      4. Straighten and creep final meters to dock bumpers

    The reverse path uses two connected arcs:
      - Arc 1: tight turn (radius ~14m) rotating trailer toward dock
      - Arc 2: counter-steer straightening arc (radius ~20m)
    """
    hx, hy = _heading_to_xy(target_heading)
    sign = 1 if approach_side == "right" else -1

    # Lateral unit vector (perpendicular, toward the setup side)
    lx, ly = sign * hy, sign * -hx

    # --- Approach corridor ---
    # Start 120m out along the dock heading, offset 5m laterally (on the yard road)
    approach_start = (120 * hx + 5 * lx, 120 * hy + 5 * ly)
    # Drive to a point ~45m past the dock, still on the yard road
    approach_end = (45 * hx + 5 * lx, 45 * hy + 5 * ly)
    approach_pts = _straight_points(approach_start, (target_heading + 180) % 360, 75, num_points=5)
    approach_speeds = np.linspace(10.0, 5.0, 5).tolist()
    approach_headings = [target_heading] * 5

    # --- Setup: angle away from dock ---
    # Pull forward while turning away — arc with radius ~18m, sweeping ~35°
    setup_arc_radius = 18.0
    # Arc center is offset perpendicular to travel direction
    # We turn away from the dock, so the arc center is on the dock side
    dock_side_lx, dock_side_ly = -lx, -ly  # opposite of approach side
    arc_center_x = approach_end[0] + dock_side_lx * setup_arc_radius
    arc_center_y = approach_end[1] + dock_side_ly * setup_arc_radius

    # Start and end angles for the setup arc (in math convention)
    start_math_angle = _compass_to_math(target_heading) + (180 if sign > 0 else 0)
    sweep = sign * 35  # degrees of arc
    setup_arc = _arc_points(
        (arc_center_x, arc_center_y),
        setup_arc_radius,
        start_math_angle,
        start_math_angle + sweep,
        num_points=4,
    )
    setup_speeds = np.linspace(4.0, 2.0, 4).tolist()
    # Cab heading rotates during the setup turn
    setup_headings = [
        (target_heading + sign * 35 * i / 3) % 360
        for i in range(4)
    ]

    # --- Stop at setup position ---
    setup_point = setup_arc[-1]
    setup_heading = setup_headings[-1]
    stop_pts = [setup_point]
    stop_speeds = [0.0]
    stop_headings = [setup_heading]

    # --- Reverse arc: two-part curve ---
    # Part 1: tight arc rotating ~55° (trailer swings toward dock)
    r1 = MIN_TURN_RADIUS + 1.5  # ~14.3m
    # Arc center for reverse turn is on the opposite side from where we turned during setup
    rev_center1_x = setup_point[0] - lx * r1
    rev_center1_y = setup_point[1] - ly * r1
    rev_start_angle = _compass_to_math(setup_heading) + (0 if sign > 0 else 180)
    rev_sweep1 = -sign * 55
    reverse_arc1 = _arc_points(
        (rev_center1_x, rev_center1_y), r1,
        rev_start_angle, rev_start_angle + rev_sweep1,
        num_points=10,
    )

    # Part 2: counter-steer straightening arc, ~25° with larger radius
    r2 = 22.0
    last_pt1 = reverse_arc1[-1]
    rev_center2_x = last_pt1[0] + lx * r2
    rev_center2_y = last_pt1[1] + ly * r2
    rev2_start_angle = rev_start_angle + rev_sweep1 + (180 if True else 0)
    rev_sweep2 = sign * 25
    reverse_arc2 = _arc_points(
        (rev_center2_x, rev_center2_y), r2,
        rev2_start_angle, rev2_start_angle + rev_sweep2,
        num_points=6,
    )

    # Final straight creep to dock (0, 0)
    if reverse_arc2:
        last_rev = reverse_arc2[-1]
    else:
        last_rev = reverse_arc1[-1]
    final_straight = _straight_points(last_rev, target_heading,
                                       math.hypot(last_rev[0], last_rev[1]),
                                       num_points=4)
    # Override last point to be exactly the dock
    final_straight[-1] = (0.0, 0.0)

    # Speeds during reverse
    n_rev = len(reverse_arc1) + len(reverse_arc2) + len(final_straight)
    reverse_speeds = []
    goal_stops = {2, 6, 12}  # GOAL stop indices (Get Out And Look)
    for i in range(n_rev):
        if i in goal_stops:
            reverse_speeds.append(0.0)
        elif i >= n_rev - 3:
            reverse_speeds.append(0.8)
        else:
            reverse_speeds.append(rng.uniform(1.5, 3.0))

    # Cab headings during reverse: cab faces away from travel direction
    # During arc1, cab rotates as it backs and steers
    reverse_all_pts = reverse_arc1 + reverse_arc2 + final_straight
    reverse_headings = []
    for i in range(len(reverse_all_pts)):
        if i < len(reverse_all_pts) - 1:
            travel_hdg = _heading_between(reverse_all_pts[i], reverse_all_pts[i + 1])
            # Cab faces roughly opposite of travel, but offset by jackknife angle
            # During tight turns the cab-trailer angle opens up
            if i < len(reverse_arc1):
                jackknife = sign * rng.uniform(10, 25)  # cab angled from trailer line
            else:
                jackknife = sign * rng.uniform(2, 8)  # straightening out
            cab_hdg = (travel_hdg + 180 + jackknife) % 360
        else:
            cab_hdg = (target_heading + 180) % 360  # facing out from dock
        reverse_headings.append(cab_hdg)

    # Assemble full path
    path = approach_pts + setup_arc + stop_pts + reverse_all_pts
    speeds = approach_speeds + setup_speeds + stop_speeds + reverse_speeds
    headings = approach_headings + setup_headings + stop_headings + reverse_headings

    return path, speeds, headings


def _blind_side(
    target_heading: float,
    rng,
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """Blind-side back — dock on passenger side, reduced visibility.

    Same geometry as alley dock (left side approach) but with:
      - More GOAL stops (driver can't see trailer in mirrors as well)
      - Slower speeds throughout
      - More heading jitter (less confident steering)
    """
    path, speeds, headings = _alley_dock(target_heading, rng, approach_side="left")

    # Slow everything down by 30%
    speeds = [max(s * 0.7, 0.0) for s in speeds]

    # Add extra GOAL stops — every 4th reverse point
    for i in range(len(speeds)):
        # Only in the reverse phase (after approach + setup + stop)
        if i > 12 and speeds[i] > 0 and i % 4 == 0:
            speeds[i] = 0.0

    return path, speeds, headings


def _pull_through(
    target_heading: float,
    rng,
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """Pull-through maneuver — no backing. Used at truck stops and fuel islands.

    Geometry:
      1. Approach on the lot road
      2. Wide sweeping arc (~25m radius, ~90° turn) into the pull-through lane
      3. Short straight to the stop position
    """
    hx, hy = _heading_to_xy(target_heading)
    # Approach from a perpendicular lot road
    perp_heading = (target_heading + 90) % 360
    phx, phy = _heading_to_xy(perp_heading)

    # Approach corridor: coming from the side, 80m out
    approach_start = (80 * phx, 80 * phy)
    arc_entry = (25 * phx, 25 * phy)
    approach_pts = _straight_points(approach_start, (perp_heading + 180) % 360, 55, num_points=4)
    approach_speeds = np.linspace(10.0, 6.0, 4).tolist()
    approach_headings = [perp_heading] * 4

    # Sweeping arc: 90° turn, radius 25m (wide for a truck)
    arc_radius = 25.0
    # Arc center: offset toward the inside of the turn
    arc_center = (arc_entry[0] - hx * arc_radius, arc_entry[1] - hy * arc_radius)
    start_angle = _compass_to_math(perp_heading)
    # Turn 90° to align with target_heading
    arc_pts = _arc_points(arc_center, arc_radius, start_angle, start_angle - 90, num_points=8)
    arc_speeds = np.linspace(5.0, 4.0, 8).tolist()
    arc_headings = [
        (perp_heading - 90 * i / 7) % 360
        for i in range(8)
    ]

    # Final straight: 15m to stop position at (0, 0)
    if arc_pts:
        straight_start = arc_pts[-1]
    else:
        straight_start = arc_entry
    final_pts = _straight_points(straight_start, (target_heading + 180) % 360,
                                  math.hypot(straight_start[0], straight_start[1]),
                                  num_points=3)
    final_pts[-1] = (0.0, 0.0)
    final_speeds = [3.0, 2.0, 0.0]
    final_headings = [target_heading, target_heading, target_heading]

    path = approach_pts + arc_pts + final_pts
    speeds = approach_speeds + arc_speeds + final_speeds
    headings = approach_headings + arc_headings + final_headings

    return path, speeds, headings


def _angle_back(
    target_heading: float,
    rng,
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """Back into an angled parking space (45-60°). Common at truck stop parking rows.

    Geometry:
      1. Approach along the parking aisle
      2. Pull past the space, stop
      3. Short reverse with ~40° heading change into the angled space
      4. Straighten and stop

    Shorter and simpler than an alley dock — the angled space reduces
    the amount of rotation needed.
    """
    hx, hy = _heading_to_xy(target_heading)
    # The parking aisle runs perpendicular to the spaces
    aisle_heading = (target_heading + 55) % 360  # aisle at ~55° to the space
    ahx, ahy = _heading_to_xy(aisle_heading)

    # Approach along aisle
    approach_start = (60 * ahx, 60 * ahy)
    approach_end = (20 * ahx, 20 * ahy)
    approach_pts = _straight_points(approach_start, (aisle_heading + 180) % 360, 40, num_points=4)
    approach_speeds = np.linspace(8.0, 3.0, 4).tolist()
    approach_headings = [aisle_heading] * 4

    # Stop
    stop_pts = [approach_end]
    stop_speeds = [0.0]
    stop_headings = [aisle_heading]

    # Short reverse arc: ~40° rotation, radius ~15m
    arc_radius = 15.0
    lx, ly = hy, -hx  # perpendicular to space heading
    arc_center = (approach_end[0] + lx * arc_radius, approach_end[1] + ly * arc_radius)
    start_angle = _compass_to_math(aisle_heading) + 180
    sweep = -40
    reverse_arc = _arc_points(arc_center, arc_radius, start_angle, start_angle + sweep, num_points=8)

    # Final straight to target
    if reverse_arc:
        last_arc = reverse_arc[-1]
    else:
        last_arc = approach_end
    final_dist = math.hypot(last_arc[0], last_arc[1])
    final_pts = _straight_points(last_arc, target_heading, final_dist, num_points=3)
    final_pts[-1] = (0.0, 0.0)

    reverse_all = reverse_arc + final_pts
    n_rev = len(reverse_all)
    reverse_speeds = []
    for i in range(n_rev):
        if i in (2, 6):
            reverse_speeds.append(0.0)  # GOAL stop
        elif i >= n_rev - 2:
            reverse_speeds.append(0.8)
        else:
            reverse_speeds.append(rng.uniform(1.5, 2.5))

    # Headings during reverse
    reverse_headings = []
    for i in range(len(reverse_all)):
        if i < len(reverse_all) - 1:
            travel_hdg = _heading_between(reverse_all[i], reverse_all[i + 1])
            cab_hdg = (travel_hdg + 180 + rng.uniform(-5, 5)) % 360
        else:
            cab_hdg = (target_heading + 180) % 360
        reverse_headings.append(cab_hdg)

    path = approach_pts + stop_pts + reverse_all
    speeds = approach_speeds + stop_speeds + reverse_speeds
    headings = approach_headings + stop_headings + reverse_headings

    return path, speeds, headings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_GENERATORS = {
    ManeuverType.STRAIGHT_BACK: _straight_back,
    ManeuverType.ALLEY_DOCK: _alley_dock,
    ManeuverType.BLIND_SIDE: _blind_side,
    ManeuverType.PULL_THROUGH: _pull_through,
    ManeuverType.ANGLE_BACK: _angle_back,
}


def generate_arrival_maneuver(
    dock_lat: float,
    dock_lon: float,
    dock_heading: float,
    start_time: datetime,
    rng,
    maneuver_type: ManeuverType = ManeuverType.ALLEY_DOCK,
    approach_side: str = "right",
) -> list[TracePoint]:
    """Generate trace points for a truck arriving at a target location.

    Args:
        dock_lat, dock_lon: Target location (dock door, parking space, fuel island).
        dock_heading: Compass heading of the road approaching the target.
        start_time: Timestamp for the first point.
        rng: Random instance.
        maneuver_type: Which parking pattern to use.
        approach_side: For alley dock — which side the dock is on.

    Returns:
        List of TracePoints ending at the target (time-ordered).
    """
    generator = _GENERATORS[maneuver_type]
    if maneuver_type in (ManeuverType.ALLEY_DOCK,):
        path, speeds, headings = generator(dock_heading, rng, approach_side)
    else:
        path, speeds, headings = generator(dock_heading, rng)

    # Heading jitter: more during backing, less during forward driving
    jitter = 2.5 if maneuver_type != ManeuverType.PULL_THROUGH else 0.5

    return _build_trace(
        path, speeds, headings,
        dock_lat, dock_lon, start_time, rng,
        heading_jitter=jitter,
    )


def generate_departure_maneuver(
    dock_lat: float,
    dock_lon: float,
    dock_heading: float,
    start_time: datetime,
    rng,
    maneuver_type: ManeuverType = ManeuverType.ALLEY_DOCK,
    approach_side: str = "right",
) -> list[TracePoint]:
    """Generate trace points for a truck departing from a target location.

    For backing maneuvers: pull forward along the reverse path, then exit.
    For pull-through: simply drive out the exit end.
    """
    if maneuver_type == ManeuverType.PULL_THROUGH:
        # Pull-through departure: just drive straight out
        path = _straight_points((0.0, 0.0), dock_heading, 80, num_points=6)
        speeds = np.linspace(0.0, 12.0, 6).tolist()
        headings = [dock_heading] * 6
        return _build_trace(
            path, speeds, headings,
            dock_lat, dock_lon, start_time, rng,
            heading_jitter=0.5,
        )

    # For all backing maneuvers: generate the arrival path, reverse it,
    # and apply a forward departure speed profile
    generator = _GENERATORS[maneuver_type]
    if maneuver_type in (ManeuverType.ALLEY_DOCK,):
        path, _, headings = generator(dock_heading, rng, approach_side)
    else:
        path, _, headings = generator(dock_heading, rng)

    # Reverse path order (dock → exit)
    path = list(reversed(path))
    headings = list(reversed(headings))

    # Flip headings 180° for forward travel (was facing backward during reverse)
    n = len(path)
    speeds = []
    new_headings = []
    for i in range(n):
        frac = i / max(n - 1, 1)
        # Speed ramp: 0 → 2 → 5 → 12 mph
        if frac < 0.05:
            speed = 0.0
        elif frac < 0.3:
            speed = 2.0 + (frac - 0.05) / 0.25 * 4.0
        elif frac < 0.6:
            speed = 6.0 + (frac - 0.3) / 0.3 * 3.0
        else:
            speed = 9.0 + (frac - 0.6) / 0.4 * 3.0
        speeds.append(speed)

        # Heading: compute from path direction (forward travel now)
        if i < n - 1:
            hdg = _heading_between(path[i], path[i + 1])
        else:
            hdg = dock_heading  # exiting toward the road
        new_headings.append(hdg)

    return _build_trace(
        path, speeds, new_headings,
        dock_lat, dock_lon, start_time, rng,
        heading_jitter=1.5,
    )
