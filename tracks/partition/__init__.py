"""
Partitions GPS traces into Valhalla-tile-aligned hive partitions so downstream
map-matching can hit a hot tile cache instead of randomly thrashing it.
"""

from .classify import (
    HILBERT_ORDER,
    LOCAL_KM,
    LONGHAUL_DEG,
    REGIONAL_KM,
    TIER_NAMES,
    TraceMetadata,
    assign_partitions_vectorized,
    classify_and_partition_key,
    metadata_from_trace_points,
)
from .tiles import (
    VALHALLA_L0_DEG,
    VALHALLA_L1_DEG,
    haversine_km,
    valhalla_l0_tile,
    valhalla_l1_tile,
    valhalla_tile_id,
)
from .writer import (
    partition_existing_parquet,
    write_partitions,
    write_trips_partitioned,
)

__all__ = [
    "HILBERT_ORDER",
    "LOCAL_KM",
    "LONGHAUL_DEG",
    "REGIONAL_KM",
    "TIER_NAMES",
    "TraceMetadata",
    "VALHALLA_L0_DEG",
    "VALHALLA_L1_DEG",
    "assign_partitions_vectorized",
    "classify_and_partition_key",
    "haversine_km",
    "metadata_from_trace_points",
    "partition_existing_parquet",
    "valhalla_l0_tile",
    "valhalla_l1_tile",
    "valhalla_tile_id",
    "write_partitions",
    "write_trips_partitioned",
]
