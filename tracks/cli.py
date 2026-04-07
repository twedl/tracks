"""CLI entry point for the tracks GPS trace generator."""

import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from .models import TripConfig
from .trace import generate_trace, traces_to_csv, traces_to_parquet


def parse_latlon(s: str) -> tuple[float, float]:
    parts = s.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Expected lat,lon but got: {s}")
    return float(parts[0]), float(parts[1])


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic GPS traces for commercial truck delivery trips."
    )
    parser.add_argument(
        "--origin", type=parse_latlon, default=None,
        help="Origin lat,lon (e.g. 41.8781,-87.6298)"
    )
    parser.add_argument(
        "--destination", type=parse_latlon, default=None,
        help="Destination lat,lon (e.g. 43.0389,-87.9065)"
    )
    parser.add_argument(
        "--random", action="store_true",
        help="Generate a random trip within Ontario (requires Valhalla)"
    )
    parser.add_argument(
        "--count", type=int, default=1,
        help="Number of back-to-back trips to generate (default: 1). "
             "Trip N starts where trip N-1 ended. Implies --random for trips 2+."
    )
    parser.add_argument(
        "--dwell", type=float, default=45.0,
        help="Dwell time in minutes between chained trips (default: 45)"
    )
    parser.add_argument(
        "--min-distance", type=float, default=50.0,
        help="Minimum trip distance in km for random legs (default: 50)"
    )
    parser.add_argument(
        "--departure", type=str, default=None,
        help="Departure time in ISO format (default: now)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output file path (default: stdout for CSV)"
    )
    parser.add_argument(
        "--noise", type=float, default=3.0,
        help="GPS noise in meters, CEP50 (default: 3.0, set 0 for clean)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--format", type=str, default="csv", choices=["csv", "parquet"],
        help="Output format (default: csv)"
    )
    parser.add_argument(
        "--partition", action="store_true",
        help="Write a hive-partitioned parquet dataset (tier=…/partition_id=…/) "
             "into the directory given by --output. Forces --format=parquet."
    )

    maneuver_choices = ["straight_back", "alley_dock", "blind_side", "pull_through", "angle_back"]
    parser.add_argument(
        "--origin-maneuver", type=str, default=None,
        choices=maneuver_choices,
        help="Parking maneuver at origin (default: alley_dock, random with --random)"
    )
    parser.add_argument(
        "--dest-maneuver", type=str, default=None,
        choices=maneuver_choices,
        help="Parking maneuver at destination (default: alley_dock, random with --random)"
    )

    args = parser.parse_args()

    if args.count > 1 and not args.random:
        if not args.origin or not args.destination:
            parser.error("--origin and --destination are required for the first trip "
                         "(or use --random)")

    if args.format == "parquet" and not args.output:
        sys.stderr.write("Error: --output is required for parquet format\n")
        sys.exit(1)

    rng = random.Random(args.seed)

    if args.departure:
        departure = datetime.fromisoformat(args.departure)
    else:
        departure = datetime.now()

    all_trips: list[tuple[list, str]] = []
    next_origin = None
    total_points = 0

    for i in range(args.count):
        is_first = i == 0

        # Determine origin and destination
        if is_first and not args.random:
            if not args.origin or not args.destination:
                parser.error("--origin and --destination are required (or use --random)")
            origin = args.origin
            destination = args.destination
            o_zone, d_zone = "specified", "specified"
        else:
            from .random_trip import generate_random_endpoints, random_maneuvers, _snap_to_road

            if next_origin:
                # Chain: use previous destination as origin, pick random dest
                origin = next_origin
                o_zone = "prev-dest"
                for _ in range(10):
                    _, destination, _, d_zone = generate_random_endpoints(
                        rng, min_distance_km=args.min_distance,
                    )
                    # Make sure we're not routing back to roughly the same spot
                    from .random_trip import _haversine_km
                    if _haversine_km(*origin, *destination) >= args.min_distance:
                        break
            else:
                origin, destination, o_zone, d_zone = generate_random_endpoints(
                    rng, min_distance_km=args.min_distance,
                )

        # Determine maneuvers
        if args.random or not is_first:
            from .random_trip import random_maneuvers
            rand_o, rand_d = random_maneuvers(rng)
            origin_maneuver = (args.origin_maneuver if is_first else None) or rand_o
            dest_maneuver = (args.dest_maneuver if is_first else None) or rand_d
        else:
            origin_maneuver = args.origin_maneuver or "alley_dock"
            dest_maneuver = args.dest_maneuver or "alley_dock"

        config = TripConfig(
            origin=origin,
            destination=destination,
            departure_time=departure,
            gps_noise_meters=args.noise,
            seed=rng.randint(0, 2**31),
            origin_maneuver=origin_maneuver,
            destination_maneuver=dest_maneuver,
        )

        print(f"Trip {i + 1}/{args.count}: {o_zone} → {d_zone}")
        print(f"  ID:          {config.trip_id}")
        print(f"  Origin:      {origin[0]:.6f}, {origin[1]:.6f}")
        print(f"  Destination: {destination[0]:.6f}, {destination[1]:.6f}")
        print(f"  Departure:   {departure.strftime('%Y-%m-%d %H:%M')}")
        print(f"  Maneuvers:   {origin_maneuver} → {dest_maneuver}")

        points = generate_trace(config)
        all_trips.append((points, config.trip_id))
        total_points += len(points)

        print(f"  Points:      {len(points)}")

        # Set up next trip in chain
        next_origin = destination
        if points:
            arrival_time = points[-1].timestamp
            dwell = timedelta(minutes=args.dwell + rng.uniform(-10, 10))
            departure = arrival_time + dwell
            print(f"  Arrived:     {arrival_time.strftime('%Y-%m-%d %H:%M')}")
            if i < args.count - 1:
                print(f"  Next depart: {departure.strftime('%Y-%m-%d %H:%M')} "
                      f"({dwell.total_seconds() / 60:.0f} min dwell)")

    # Write output
    if args.partition:
        if not args.output:
            parser.error("--partition requires --output to be set to a directory")
        from .gps_partition import write_trips_partitioned
        summary = write_trips_partitioned(all_trips, Path(args.output))
        tier_summary = ", ".join(f"{tier}={n}" for tier, n in sorted(summary.items()))
        print(
            f"\nWrote {total_points} points ({args.count} trips) to {args.output} "
            f"[partitions: {tier_summary or 'none'}]"
        )
    elif args.format == "parquet":
        traces_to_parquet(all_trips, args.output)
        print(f"\nWrote {total_points} points ({args.count} trips) to {args.output}")
    else:
        csv_str = traces_to_csv(all_trips, args.output)
        if not args.output:
            sys.stdout.write(csv_str)
        else:
            print(f"\nWrote {total_points} points ({args.count} trips) to {args.output}")


if __name__ == "__main__":
    main()
