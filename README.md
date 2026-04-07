# tracks

Generate realistic GPS traces for commercial truck delivery trips.

`tracks` routes trips through a local [Valhalla](https://github.com/valhalla/valhalla)
instance, then layers on speed profiles, parking maneuvers, and configurable GPS
noise to produce point-by-point traces suitable for testing map-matching, ETA
models, telematics pipelines, and similar systems.

It also includes a partitioner (`tracks.gps_partition`) that rewrites a flat
parquet of GPS points into a hive-partitioned dataset aligned with Valhalla's
tile grid, so downstream map-matching can hit a hot tile cache.

## Install

```bash
git clone <this-repo>
cd tracks
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

Python 3.10+.

## Running Valhalla locally

Trip generation needs a routing backend. `start_valhalla.sh` brings up a
Dockerized Valhalla pre-loaded with the Ontario OSM extract:

```bash
./start_valhalla.sh
```

The first run downloads `ontario-latest.osm.pbf` from Geofabrik (~888 MB) into
`custom_files/` and builds routing tiles (~15-30 min). Subsequent runs reuse
the cached tiles. The `custom_files/` directory is gitignored.

## CLI

Generate a single trip between two points:

```bash
tracks --origin 43.6532,-79.3832 --destination 42.9849,-81.2453 --output trip.csv
```

Generate a chain of random trips within Ontario:

```bash
tracks --random --count 5 --output trips.parquet --format parquet
```

Write a hive-partitioned dataset (one directory per Valhalla tile bucket):

```bash
tracks --random --count 100 --output ./dataset --partition
```

Or partition an existing flat parquet of GPS points:

```bash
tracks-partition input.parquet ./dataset
```

See `examples/gta_to_401_truck_stop.py` for a scripted example.

## License

MIT — see [LICENSE](LICENSE).
