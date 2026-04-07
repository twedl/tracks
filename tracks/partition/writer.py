"""
Hive-partitioned dataset writer and public entry points.

Layout (explicit hive partitioning):
    <output>/
        tier=local/    partition_id=<id>/<file>.parquet
        tier=regional/ partition_id=<id>/<file>.parquet
        tier=longhaul/ partition_id=<id>/<file>.parquet

Each row is a single GPS point keyed by `id` (the trip identifier used
elsewhere in the project). Within a partition file rows are sorted by a
Hilbert-curve index over the trip centroid for spatial locality.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from ..generate.models import TracePoint
from .classify import assign_partitions_vectorized, metadata_from_trace_points

# Columns the per-point parquet files keep (the schema readers see *inside*
# each file). `tier` and `partition_id` come from the directory names and are
# stripped from the row data by pyarrow.dataset.write_dataset.
_POINT_COLUMNS = ["id", "lat", "lon", "speed", "heading", "timestamp"]


def write_partitions(
    metadata: pd.DataFrame,
    points: pd.DataFrame,
    output_dir: Path,
) -> None:
    """
    Write a hive-partitioned dataset rooted at `output_dir`.

    `metadata` must contain (id, tier, partition_id, hilbert_idx).
    `points`   must contain (id, lat, lon, speed, heading, timestamp).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    merged = points.merge(
        metadata[["id", "tier", "partition_id", "hilbert_idx"]],
        on="id",
    )
    # Sort by hilbert_idx so each partition file gets spatially-coherent rows.
    merged = merged.sort_values(["partition_id", "hilbert_idx"])
    merged = merged.drop(columns=["hilbert_idx"])

    table = pa.Table.from_pandas(merged, preserve_index=False)

    partitioning = ds.partitioning(
        pa.schema([("tier", pa.string()), ("partition_id", pa.int64())]),
        flavor="hive",
    )
    ds.write_dataset(
        table,
        base_dir=str(output_dir),
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="overwrite_or_ignore",
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def _trips_to_points_df(trips: list[tuple[list[TracePoint], str]]) -> pd.DataFrame:
    """Flatten (points, trip_id) tuples into a one-row-per-point DataFrame."""
    ids, lats, lons, speeds, headings, timestamps = [], [], [], [], [], []
    for points, trip_id in trips:
        for pt in points:
            ids.append(trip_id)
            lats.append(pt.lat)
            lons.append(pt.lon)
            speeds.append(round(pt.speed_mph, 1))
            headings.append(round(pt.heading, 1))
            timestamps.append(pt.timestamp)
    return pd.DataFrame({
        "id": ids, "lat": lats, "lon": lons,
        "speed": speeds, "heading": headings, "timestamp": timestamps,
    })


def write_trips_partitioned(
    trips: list[tuple[list[TracePoint], str]],
    output_dir: Path,
) -> dict[str, int]:
    """
    Write in-memory trips as a hive-partitioned parquet dataset.

    Returns a {tier_name: partition_count} summary.
    """
    metadata_rows = [metadata_from_trace_points(tid, pts) for pts, tid in trips if pts]
    metadata = pd.DataFrame([m.__dict__ for m in metadata_rows])
    metadata = assign_partitions_vectorized(metadata)

    points = _trips_to_points_df(trips)
    write_partitions(metadata, points, output_dir)

    return (
        metadata.drop_duplicates("partition_id")
        .groupby("tier")
        .size()
        .to_dict()
    )


def partition_existing_parquet(input_path: Path, output_dir: Path) -> dict[str, int]:
    """
    Read a flat parquet (one row per GPS point with `id, lat, lon, ...`)
    and rewrite it as a hive-partitioned dataset.

    Returns a {tier_name: partition_count} summary.
    """
    input_path = Path(input_path)
    points = pd.read_parquet(input_path)

    required = {"id", "lat", "lon"}
    missing = required - set(points.columns)
    if missing:
        raise ValueError(
            f"{input_path} is missing required columns: {sorted(missing)}"
        )

    # Per-trip centroid + bbox diagonal in one groupby pass.
    agg = points.groupby("id").agg(
        lat_min=("lat", "min"),
        lat_max=("lat", "max"),
        lon_min=("lon", "min"),
        lon_max=("lon", "max"),
        centroid_lat=("lat", "mean"),
        centroid_lon=("lon", "mean"),
    ).reset_index()

    # Vectorized haversine for the bbox diagonal.
    R = 6371.0
    lat1 = np.radians(agg["lat_min"].to_numpy())
    lat2 = np.radians(agg["lat_max"].to_numpy())
    dlat = lat2 - lat1
    dlon = np.radians(agg["lon_max"].to_numpy() - agg["lon_min"].to_numpy())
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    agg["bbox_diag_km"] = R * 2 * np.arcsin(np.sqrt(a))

    metadata = agg[["id", "centroid_lat", "centroid_lon", "bbox_diag_km"]]
    metadata = assign_partitions_vectorized(metadata)

    write_partitions(metadata, points, output_dir)

    return (
        metadata.drop_duplicates("partition_id")
        .groupby("tier")
        .size()
        .to_dict()
    )
