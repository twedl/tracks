"""Trip generation: routing, interpolation, noise, parking, and CLI."""

from .models import RouteSegment, TracePoint, TripConfig
from .trace import generate_trace, traces_to_csv, traces_to_parquet

__all__ = [
    "RouteSegment",
    "TracePoint",
    "TripConfig",
    "generate_trace",
    "traces_to_csv",
    "traces_to_parquet",
]
