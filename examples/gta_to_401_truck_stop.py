"""Example: GTA logistics facility to truck stop between London and Windsor.

Origin:  Walmart Distribution Centre, Mississauga
         (Britannia Rd E & Hwy 407 area — major logistics corridor)

Destination: Petro-Canada, Comber, ON
             (Hwy 401 at Exit 48 — truck stop between London and Windsor)

Distance: ~320 km via 401, roughly 3.5 hours for a truck.
"""

from datetime import datetime

from tracks.models import TripConfig
from tracks.trace import generate_trace, trace_to_csv


def main():
    config = TripConfig(
        # Walmart DC, Mississauga — Britannia Rd E near 407/401 interchange
        origin=(43.6032, -79.6726),
        # Petro-Canada, Comber ON — 401 at Exit 48
        destination=(42.238695, -82.550094),
        departure_time=datetime(2026, 4, 6, 6, 0, 0),
        gps_noise_meters=3.0,
        seed=42,
        origin_maneuver="alley_dock",        # backing out of DC dock
        destination_maneuver="pull_through",  # pulling into truck stop fuel island
    )

    print(f"Generating trace: Mississauga DC → Comber Petro-Canada")
    print(f"Trip ID: {config.trip_id}")
    print(f"Departure: {config.departure_time}")

    points = generate_trace(config)

    output_file = "gta_to_comber.csv"
    trace_to_csv(points, config.trip_id, output_file)

    print(f"Wrote {len(points)} GPS points to {output_file}")

    # Summary stats
    if points:
        duration_min = (points[-1].timestamp - points[0].timestamp).total_seconds() / 60
        speeds = [p.speed_mph for p in points]
        print(f"Duration: {duration_min:.0f} minutes")
        print(f"Speed range: {min(speeds):.1f} — {max(speeds):.1f} mph")

        # Count parking points (speed < 5 mph at start/end)
        parking_start = sum(1 for p in points[:30] if p.speed_mph < 5)
        parking_end = sum(1 for p in points[-30:] if p.speed_mph < 5)
        print(f"Low-speed points in first 30: {parking_start} (departure maneuver)")
        print(f"Low-speed points in last 30: {parking_end} (arrival maneuver)")


if __name__ == "__main__":
    main()
