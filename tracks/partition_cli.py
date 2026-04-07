"""CLI entry point for partitioning an existing flat-parquet GPS dataset."""

import argparse
import sys
from pathlib import Path

from .gps_partition import partition_existing_parquet


def main():
    parser = argparse.ArgumentParser(
        description="Rewrite a flat GPS parquet file as a hive-partitioned "
                    "dataset (tier=…/partition_id=…/) for Valhalla cache locality."
    )
    parser.add_argument(
        "input", type=Path,
        help="Input parquet file with at least `id, lat, lon` columns."
    )
    parser.add_argument(
        "output", type=Path,
        help="Output directory for the partitioned dataset (will be created)."
    )
    args = parser.parse_args()

    if not args.input.is_file():
        parser.error(f"Input parquet not found: {args.input}")

    summary = partition_existing_parquet(args.input, args.output)
    tier_summary = ", ".join(f"{tier}={n}" for tier, n in sorted(summary.items()))
    print(
        f"Wrote partitioned dataset to {args.output} "
        f"[partitions: {tier_summary or 'none'}]"
    )


if __name__ == "__main__":
    sys.exit(main())
