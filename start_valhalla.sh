#!/bin/bash
# Start local Valhalla routing service with Ontario OSM data.
#
# Prerequisites:
#   - Docker Desktop installed and running
#   - Ontario PBF downloaded to ./custom_files/ontario-latest.osm.pbf
#
# First run will build routing tiles from the PBF (~15-30 min for Ontario).
# Subsequent runs reuse cached tiles and start in seconds.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/custom_files"
PBF_FILE="$DATA_DIR/ontario-latest.osm.pbf"
CONTAINER_NAME="valhalla-ontario"

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH."
    echo "Install Docker Desktop: brew install --cask docker"
    exit 1
fi

if ! docker info &> /dev/null 2>&1; then
    echo "ERROR: Docker daemon is not running. Start Docker Desktop first."
    exit 1
fi

if [ ! -f "$PBF_FILE" ]; then
    echo "Ontario PBF not found at $PBF_FILE"
    echo "Downloading from Geofabrik (~888 MB)..."
    curl -L -o "$PBF_FILE" https://download.geofabrik.de/north-america/canada/ontario-latest.osm.pbf
fi

# Stop existing container if running
if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
    echo "Stopping existing $CONTAINER_NAME container..."
    docker stop "$CONTAINER_NAME" > /dev/null
fi
if docker ps -aq -f name="$CONTAINER_NAME" | grep -q .; then
    docker rm "$CONTAINER_NAME" > /dev/null
fi

echo "Starting Valhalla with Ontario data..."
echo "First run builds routing tiles — this takes 15-30 minutes."
echo "Subsequent runs start in seconds."
echo ""

docker run -d \
    --name "$CONTAINER_NAME" \
    -p 8002:8002 \
    -v "$DATA_DIR:/custom_files" \
    -e tile_urls=http://download.geofabrik.de/north-america/canada/ontario-latest.osm.pbf \
    -e use_tiles_ignore_pbf=True \
    -e serve_tiles=True \
    -e build_admins=True \
    ghcr.io/gis-ops/docker-valhalla/valhalla:latest

echo ""
echo "Container '$CONTAINER_NAME' started."
echo "Waiting for Valhalla to become ready..."

# Wait for health
MAX_WAIT=1800  # 30 min for tile building
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if curl -s http://localhost:8002/status > /dev/null 2>&1; then
        echo ""
        echo "Valhalla is ready at http://localhost:8002"
        echo ""
        echo "Test with:"
        echo '  curl -s http://localhost:8002/route -d '\''{"locations":[{"lat":43.7,"lon":-79.4},{"lat":43.0,"lon":-81.2}],"costing":"truck"}'\'' | python3 -m json.tool | head -20'
        exit 0
    fi
    sleep 10
    WAITED=$((WAITED + 10))
    # Show progress from container logs
    LAST_LOG=$(docker logs --tail 1 "$CONTAINER_NAME" 2>&1)
    printf "\r  [%ds] %s" "$WAITED" "$LAST_LOG"
done

echo ""
echo "WARNING: Valhalla did not become ready within ${MAX_WAIT}s."
echo "Check logs: docker logs $CONTAINER_NAME"
exit 1
