"""
gps_partition.py

Partitions GPS traces into Valhalla-tile-aligned hive partitions so downstream
map-matching can hit a hot tile cache instead of randomly thrashing it.

Layout (explicit hive partitioning):
    <output>/
        tier=local/    partition_id=<id>/<file>.parquet
        tier=regional/ partition_id=<id>/<file>.parquet
        tier=longhaul/ partition_id=<id>/<file>.parquet

Tiers:
    local    (< 100 km bbox)   → Valhalla L1 (1°×1°) tile bucket
    regional (100–800 km bbox) → Valhalla L0 (4°×4°) tile bucket
    longhaul (> 800 km bbox)   → coarse 8°×8° super-region bucket

Each row is a single GPS point keyed by `id` (the trip identifier used
elsewhere in the project). Within a partition file rows are sorted by a
Hilbert-curve index over the trip centroid for spatial locality.
"""

import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from hilbertcurve.hilbertcurve import HilbertCurve

from .models import TracePoint

# ---------------------------------------------------------------------------
# Valhalla tile functions
# These replicate Valhalla's GraphId tile logic for L0 and L1.
# Source: valhalla/baldr/graphid.h
# ---------------------------------------------------------------------------

VALHALLA_L1_DEG = 1.0   # 1° × 1° tiles
VALHALLA_L0_DEG = 4.0   # 4° × 4° tiles

def valhalla_tile_id(lat: float, lon: float, tile_deg: float) -> int:
    """
    Compute a flat tile index matching Valhalla's tile numbering.

    Valhalla numbers tiles row-major from the bottom-left (-90, -180):
        col = floor((lon + 180) / tile_deg)
        row = floor((lat  +  90) / tile_deg)
        id  = row * n_cols + col
    """
    n_cols = int(360 / tile_deg)
    col = int((lon + 180.0) / tile_deg)
    row = int((lat  +  90.0) / tile_deg)
    col = max(0, min(col, n_cols - 1))
    row = max(0, min(row, int(180 / tile_deg) - 1))
    return row * n_cols + col

def valhalla_l1_tile(lat: float, lon: float) -> int:
    return valhalla_tile_id(lat, lon, VALHALLA_L1_DEG)

def valhalla_l0_tile(lat: float, lon: float) -> int:
    return valhalla_tile_id(lat, lon, VALHALLA_L0_DEG)


# ---------------------------------------------------------------------------
# Haversine bounding-box diagonal
# ---------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Hilbert curve index for spatial ordering within partitions
# ---------------------------------------------------------------------------

# p=12 → 2^12 = 4096 grid cells per axis. Over the US+Canada bbox this is
# ~0.015° per cell, ~1.6 km — fine enough for ordering, cheap to compute.
HILBERT_ORDER = 12
_hc = HilbertCurve(p=HILBERT_ORDER, n=2)

# Bounding box for US + Canada
LAT_MIN, LAT_MAX = 24.0, 84.0
LON_MIN, LON_MAX = -141.0, -52.0


# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------

LOCAL_KM    = 100.0   # < 100 km  → L1 tile partition
REGIONAL_KM = 800.0   # 100–800   → L0 tile partition
                      # > 800     → long-haul partition

# Long-haul super-regions: coarse 8°×8° buckets so we don't create one giant
# partition. Long-haul traces touching the same super-region share highway tiles.
LONGHAUL_DEG = 8.0

TIER_NAMES = ("local", "regional", "longhaul")


def classify_and_partition_key(
    centroid_lat: float,
    centroid_lon: float,
    bbox_diag_km: float,
) -> tuple[str, int]:
    """
    Returns (tier_name, partition_id).

    partition_id encodes the tier in the high bits so values don't collide
    across tiers:
        bits 62–60: tier  (0=local, 1=regional, 2=longhaul)
        bits 59–0:  tile index
    """
    if bbox_diag_km < LOCAL_KM:
        tier = 0
        tile = valhalla_l1_tile(centroid_lat, centroid_lon)
    elif bbox_diag_km < REGIONAL_KM:
        tier = 1
        tile = valhalla_l0_tile(centroid_lat, centroid_lon)
    else:
        tier = 2
        tile = valhalla_tile_id(centroid_lat, centroid_lon, LONGHAUL_DEG)

    partition_id = (tier << 60) | tile
    return TIER_NAMES[tier], partition_id


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

@dataclass
class TraceMetadata:
    id: str
    centroid_lat: float
    centroid_lon: float
    bbox_diag_km: float


def metadata_from_trace_points(trip_id: str, points: list[TracePoint]) -> TraceMetadata:
    """Compute centroid + bbox-diagonal metadata from in-memory TracePoints."""
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    return TraceMetadata(
        id=trip_id,
        centroid_lat=sum(lats) / len(lats),
        centroid_lon=sum(lons) / len(lons),
        bbox_diag_km=haversine_km(min(lats), min(lons), max(lats), max(lons)),
    )


# ---------------------------------------------------------------------------
# Vectorized partition assignment (used for both in-memory and on-disk paths)
# ---------------------------------------------------------------------------

def assign_partitions_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add `tier`, `partition_id`, `hilbert_idx` columns to a metadata DataFrame.

    Input columns: id, centroid_lat, centroid_lon, bbox_diag_km
    """
    assert {"id", "centroid_lat", "centroid_lon", "bbox_diag_km"}.issubset(df.columns)

    lat = df["centroid_lat"].to_numpy()
    lon = df["centroid_lon"].to_numpy()
    diag = df["bbox_diag_km"].to_numpy()

    def tile_vec(lat_v, lon_v, deg):
        n_cols = int(360 / deg)
        col = np.floor((lon_v + 180.0) / deg).astype(np.int64)
        row = np.floor((lat_v +  90.0) / deg).astype(np.int64)
        col = np.clip(col, 0, n_cols - 1)
        row = np.clip(row, 0, int(180 / deg) - 1)
        return row * n_cols + col

    l1 = tile_vec(lat, lon, VALHALLA_L1_DEG)
    l0 = tile_vec(lat, lon, VALHALLA_L0_DEG)
    lh = tile_vec(lat, lon, LONGHAUL_DEG)

    tier = np.where(diag < LOCAL_KM, 0, np.where(diag < REGIONAL_KM, 1, 2))
    tile = np.where(tier == 0, l1, np.where(tier == 1, l0, lh))

    partition_id = (tier.astype(np.int64) << 60) | tile.astype(np.int64)
    tier_name = np.where(tier == 0, "local", np.where(tier == 1, "regional", "longhaul"))

    n = 2 ** HILBERT_ORDER - 1
    x = np.clip((lon - LON_MIN) / (LON_MAX - LON_MIN) * n, 0, n).astype(int)
    y = np.clip((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * n, 0, n).astype(int)
    hilbert_idx = np.array(_hc.distances_from_points(list(zip(x.tolist(), y.tolist()))))

    result = df.copy()
    result["tier"] = tier_name
    result["partition_id"] = partition_id
    result["hilbert_idx"] = hilbert_idx
    return result


# ---------------------------------------------------------------------------
# Hive-partitioned write
# ---------------------------------------------------------------------------

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
