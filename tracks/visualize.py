"""Visualize a GPS trace CSV on an OSM map using Folium (Leaflet.js)."""

import csv
import http.server
import os
import threading
import webbrowser
from datetime import datetime

import folium


def load_trace(csv_path: str) -> list[dict]:
    with open(csv_path) as f:
        return list(csv.DictReader(f))


def build_map(rows: list[dict]) -> folium.Map:
    lats = [float(r["lat"]) for r in rows]
    lons = [float(r["lon"]) for r in rows]
    speeds = [float(r["speed"]) for r in rows]
    center = [sum(lats) / len(lats), sum(lons) / len(lons)]

    # Use explicit tile URL with Referer meta tag to comply with OSM tile usage policy.
    # https://wiki.openstreetmap.org/wiki/Blocked_tiles
    # - Valid User-Agent: handled by the browser automatically
    # - Referer header: injected via meta tag so localhost serving works
    # - Attribution: explicit in the tile layer
    # - Caching: browser default caching is fine (no no-cache headers)
    m = folium.Map(location=center, zoom_start=8, tiles=None)

    folium.TileLayer(
        tiles="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        name="OpenStreetMap",
        max_zoom=19,
    ).add_to(m)

    # Inject Referrer-Policy so tile requests include origin even from localhost
    m.get_root().header.add_child(folium.Element(
        '<meta name="referrer" content="origin-when-cross-origin">'
    ))

    # Fit bounds
    m.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]])

    # Color segments by speed
    def speed_color(mph: float) -> str:
        if mph < 1:
            return "#d32f2f"  # red — stopped
        elif mph < 10:
            return "#f57c00"  # orange — parking maneuver
        elif mph < 30:
            return "#fbc02d"  # yellow — urban
        elif mph < 55:
            return "#66bb6a"  # light green — arterial
        else:
            return "#1565c0"  # blue — highway

    # Draw route as colored segments
    for i in range(len(rows) - 1):
        lat1, lon1 = float(rows[i]["lat"]), float(rows[i]["lon"])
        lat2, lon2 = float(rows[i + 1]["lat"]), float(rows[i + 1]["lon"])
        spd = float(rows[i]["speed"])
        folium.PolyLine(
            [[lat1, lon1], [lat2, lon2]],
            color=speed_color(spd),
            weight=4,
            opacity=0.85,
        ).add_to(m)

    # Markers for start, end, and any stops
    _add_marker(m, rows[0], "Departure (dock)", "green", "play")
    _add_marker(m, rows[-1], "Arrival (dock)", "red", "stop")

    # Mark points where truck stopped (speed 0 during driving phase)
    for i, r in enumerate(rows):
        if float(r["speed"]) < 0.5 and 20 < i < len(rows) - 20:
            folium.CircleMarker(
                [float(r["lat"]), float(r["lon"])],
                radius=4,
                color="#d32f2f",
                fill=True,
                popup=f"Stop @ {r['timestamp']}",
            ).add_to(m)

    # GPS point markers (small dots with popups)
    points_group = folium.FeatureGroup(name="GPS Points", show=False)
    for r in rows:
        folium.CircleMarker(
            [float(r["lat"]), float(r["lon"])],
            radius=2,
            color="#333",
            fill=True,
            fill_opacity=0.6,
            popup=(
                f"<b>{r['timestamp']}</b><br>"
                f"Speed: {r['speed']} mph<br>"
                f"Heading: {r['heading']}°<br>"
                f"({r['lat']}, {r['lon']})"
            ),
        ).add_to(points_group)
    points_group.add_to(m)

    # Parking detail insets — zoom circles around start/end
    _add_parking_inset(m, rows[:30], "Departure Maneuver", "green")
    _add_parking_inset(m, rows[-30:], "Arrival Maneuver", "red")

    # Legend
    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
         background:white; padding:12px 16px; border-radius:8px;
         box-shadow:0 2px 8px rgba(0,0,0,0.3); font-family:sans-serif; font-size:13px;">
      <b>Speed</b><br>
      <span style="color:#1565c0;">&#9644;</span> Highway (55+ mph)<br>
      <span style="color:#66bb6a;">&#9644;</span> Arterial (30-55)<br>
      <span style="color:#fbc02d;">&#9644;</span> Urban (10-30)<br>
      <span style="color:#f57c00;">&#9644;</span> Parking (&lt;10)<br>
      <span style="color:#d32f2f;">&#9644;</span> Stopped (0)<br>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Layer control to toggle GPS points
    folium.LayerControl().add_to(m)

    return m


def _add_marker(m, row, label, color, icon):
    folium.Marker(
        [float(row["lat"]), float(row["lon"])],
        popup=f"<b>{label}</b><br>{row['timestamp']}<br>Speed: {row['speed']} mph",
        icon=folium.Icon(color=color, icon=icon, prefix="fa"),
    ).add_to(m)


def _add_parking_inset(m, rows, label, color):
    """Add a feature group with detailed parking maneuver visualization."""
    group = folium.FeatureGroup(name=label, show=True)
    for i, r in enumerate(rows):
        spd = float(r["speed"])
        hdg = float(r["heading"])
        lat, lon = float(r["lat"]), float(r["lon"])

        # Arrow showing heading direction
        import math
        arrow_len = 0.00015  # ~15m in lat degrees
        end_lat = lat + arrow_len * math.cos(math.radians(hdg))
        end_lon = lon + arrow_len * math.sin(math.radians(hdg))

        folium.PolyLine(
            [[lat, lon], [end_lat, end_lon]],
            color=color,
            weight=2,
            opacity=0.5,
        ).add_to(group)

        folium.CircleMarker(
            [lat, lon],
            radius=3,
            color=color,
            fill=True,
            popup=f"<b>#{i}</b> {r['timestamp']}<br>Spd: {spd} mph | Hdg: {hdg}°",
        ).add_to(group)

    group.add_to(m)


def serve_map(html_path: str, port: int = 8080):
    """Serve the HTML file and open in browser."""
    directory = os.path.dirname(os.path.abspath(html_path))
    filename = os.path.basename(html_path)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)
        def log_message(self, format, *args):
            pass  # suppress logs

    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}/{filename}"
    print(f"Serving map at {url}")
    print("Press Ctrl+C to stop")

    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Visualize a truck GPS trace on an OSM map")
    parser.add_argument("csv_file", help="Path to the trace CSV file")
    parser.add_argument("--port", type=int, default=8080, help="Local server port (default: 8080)")
    parser.add_argument("--no-serve", action="store_true", help="Generate HTML only, don't start server")
    args = parser.parse_args()

    rows = load_trace(args.csv_file)
    print(f"Loaded {len(rows)} GPS points from {args.csv_file}")

    m = build_map(rows)

    html_path = args.csv_file.rsplit(".", 1)[0] + "_map.html"
    m.save(html_path)
    print(f"Map saved to {html_path}")

    if not args.no_serve:
        serve_map(html_path, args.port)


if __name__ == "__main__":
    main()
