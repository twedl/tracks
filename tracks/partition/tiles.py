"""
Valhalla-compatible tile indexing and bbox-diagonal helpers.

The tile functions replicate Valhalla's GraphId tile logic for L0 and L1.
Source: valhalla/baldr/graphid.h
"""

import math

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


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))
