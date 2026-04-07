"""
Tier classification and vectorized partition-key assignment.

Tiers:
    local    (< 100 km bbox)   → Valhalla L1 (1°×1°) tile bucket
    regional (100–800 km bbox) → Valhalla L0 (4°×4°) tile bucket
    longhaul (> 800 km bbox)   → coarse 8°×8° super-region bucket
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from hilbertcurve.hilbertcurve import HilbertCurve

from ..generate.models import TracePoint
from .tiles import (
    VALHALLA_L0_DEG,
    VALHALLA_L1_DEG,
    haversine_km,
    valhalla_l0_tile,
    valhalla_l1_tile,
    valhalla_tile_id,
)

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
