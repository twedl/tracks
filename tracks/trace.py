"""Orchestrator: chains parking, driving, and noise phases into a complete trace."""

import csv
import random
from io import BytesIO, StringIO

from .interpolator import bearing, interpolate_route
from .models import RouteSegment, TracePoint, TripConfig
from .noise import apply_noise
from .parking import ManeuverType, generate_arrival_maneuver, generate_departure_maneuver
from .router import fetch_route


def generate_trace(config: TripConfig) -> list[TracePoint]:
    """Generate a complete GPS trace for a truck delivery trip."""
    rng = random.Random(config.seed)

    # Fetch route
    route = fetch_route(config.origin, config.destination)

    # Determine dock headings from route geometry
    origin_heading = _route_start_heading(route)
    dest_heading = _route_end_heading(route)

    origin_maneuver = ManeuverType(config.origin_maneuver)
    dest_maneuver = ManeuverType(config.destination_maneuver)

    all_points: list[TracePoint] = []

    # Phase 1: Departure parking maneuver at origin (pull away from dock)
    departure_pts = generate_departure_maneuver(
        dock_lat=config.origin[0],
        dock_lon=config.origin[1],
        dock_heading=origin_heading,
        start_time=config.departure_time,
        rng=rng,
        maneuver_type=origin_maneuver,
    )
    all_points.extend(departure_pts)

    # Phase 2: Main route driving
    if departure_pts:
        driving_start = departure_pts[-1].timestamp
    else:
        driving_start = config.departure_time

    driving_pts = interpolate_route(route, driving_start, rng)
    all_points.extend(driving_pts)

    # Phase 3: Arrival parking maneuver at destination
    if driving_pts:
        arrival_start = driving_pts[-1].timestamp
    else:
        arrival_start = driving_start

    arrival_pts = generate_arrival_maneuver(
        dock_lat=config.destination[0],
        dock_lon=config.destination[1],
        dock_heading=dest_heading,
        start_time=arrival_start,
        rng=rng,
        maneuver_type=dest_maneuver,
    )
    all_points.extend(arrival_pts)

    # Apply GPS noise
    all_points = apply_noise(all_points, config.gps_noise_meters, rng)

    return all_points


def _csv_rows(points: list[TracePoint], trip_id: str) -> list[list[str]]:
    """Convert trace points to CSV row data (without header)."""
    return [
        [
            trip_id,
            f"{pt.lat:.6f}",
            f"{pt.lon:.6f}",
            f"{pt.speed_mph:.1f}",
            f"{pt.heading:.1f}",
            pt.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ]
        for pt in points
    ]


def trace_to_csv(points: list[TracePoint], trip_id: str, output_path: str | None = None) -> str:
    """Write trace points to CSV. Returns CSV string; writes to file if path given."""
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "lat", "lon", "speed", "heading", "timestamp"])
    writer.writerows(_csv_rows(points, trip_id))
    csv_str = buf.getvalue()

    if output_path:
        with open(output_path, "w") as f:
            f.write(csv_str)

    return csv_str


def traces_to_csv(
    trips: list[tuple[list[TracePoint], str]], output_path: str | None = None
) -> str:
    """Write multiple trips to a single CSV. Each trip is a (points, trip_id) tuple."""
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "lat", "lon", "speed", "heading", "timestamp"])
    for points, trip_id in trips:
        writer.writerows(_csv_rows(points, trip_id))
    csv_str = buf.getvalue()

    if output_path:
        with open(output_path, "w") as f:
            f.write(csv_str)

    return csv_str


def trace_to_parquet(points: list[TracePoint], trip_id: str, output_path: str | None = None) -> bytes:
    """Write trace points to Parquet. Returns bytes; writes to file if path given."""
    return traces_to_parquet([(points, trip_id)], output_path)


def traces_to_parquet(
    trips: list[tuple[list[TracePoint], str]], output_path: str | None = None
) -> bytes:
    """Write multiple trips to a single Parquet file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    ids, lats, lons, speeds, headings, timestamps = [], [], [], [], [], []
    for points, trip_id in trips:
        for pt in points:
            ids.append(trip_id)
            lats.append(pt.lat)
            lons.append(pt.lon)
            speeds.append(round(pt.speed_mph, 1))
            headings.append(round(pt.heading, 1))
            timestamps.append(pt.timestamp)

    table = pa.table({
        "id": ids, "lat": lats, "lon": lons,
        "speed": speeds, "heading": headings, "timestamp": timestamps,
    })

    buf = BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()

    if output_path:
        with open(output_path, "wb") as f:
            f.write(data)

    return data


def _route_start_heading(route: RouteSegment) -> float:
    """Heading of the first segment of the route."""
    if len(route.coords) < 2:
        return 0.0
    return bearing(
        route.coords[0][0], route.coords[0][1],
        route.coords[1][0], route.coords[1][1],
    )


def _route_end_heading(route: RouteSegment) -> float:
    """Heading of the last segment of the route."""
    if len(route.coords) < 2:
        return 0.0
    return bearing(
        route.coords[-2][0], route.coords[-2][1],
        route.coords[-1][0], route.coords[-1][1],
    )
