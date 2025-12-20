import streamlit as st
import pandas as pd
import json
import numpy as np

from utils.db import get_connection
from utils.formatters import format_currency, format_number

# Access shared state (initializes if needed)
conn, _, GOLD_BUCKET = get_connection()

# Metric configuration
METRICS = {
    "Net Taxes per Sq Ft": "net_taxes_per_sqft_lot",
    "Land Value per Sq Ft": "land_value_per_sqft_lot",
    "Land Value Alignment Index": "land_value_alignment_index",
}

# Magma colormap stops (normalized position, RGB) - reversed
# Perceptually uniform gradient: light -> orange -> magenta -> purple -> dark
MAGMA_STOPS = [
    (0.0, [252, 253, 191]),
    (0.25, [252, 143, 89]),
    (0.5, [183, 55, 121]),
    (0.75, [82, 22, 108]),
    (1.0, [0, 0, 4]),
]


def interpolate_magma_color(norm_val: float) -> list[int]:
    """Interpolate RGB color from magma colormap based on normalized value (0-1)."""
    # Find the two stops to interpolate between
    for i in range(len(MAGMA_STOPS) - 1):
        pos1, color1 = MAGMA_STOPS[i]
        pos2, color2 = MAGMA_STOPS[i + 1]
        if pos1 <= norm_val <= pos2:
            # Linear interpolation between stops
            t = (norm_val - pos1) / (pos2 - pos1)
            return [
                int(color1[0] + t * (color2[0] - color1[0])),
                int(color1[1] + t * (color2[1] - color1[1])),
                int(color1[2] + t * (color2[2] - color1[2])),
            ]
    # Fallback to last color
    return list(MAGMA_STOPS[-1][1])


@st.cache_data(ttl=600)
def load_map_data() -> pd.DataFrame:
    """Load parcel data for map visualization."""
    query = f"""
    SELECT
        site_parcel_id,
        parcel_address,
        geom_4326_geojson,
        current_land_value,
        current_improvement_value,
        current_total_value,
        net_taxes,
        lot_size,
        net_taxes_per_sqft_lot,
        land_value_per_sqft_lot,
        land_value_alignment_index
    FROM read_parquet('{GOLD_BUCKET}/fact_sites.parquet')
    WHERE parcel_year = (
        SELECT MAX(parcel_year)
        FROM read_parquet('{GOLD_BUCKET}/fact_sites.parquet')
    )
    AND geom_4326_geojson IS NOT NULL
    AND net_taxes > 0
    AND current_total_value > 0
    """
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Error loading map data: {str(e)}")
        return pd.DataFrame()


def calculate_colors(values: np.ndarray) -> tuple[list, float, float]:
    """
    Calculate RGBA colors using percentile normalization.

    Returns:
        Tuple of (colors list, p2 value, p98 value)
    """
    # Use 2nd and 98th percentile to clip outliers
    valid_values = values[~np.isnan(values)]
    if len(valid_values) == 0:
        return [[128, 128, 128, 100]] * len(values), 0, 0

    p2, p98 = np.nanpercentile(valid_values, [2, 98])

    # Handle edge case where p2 == p98
    if p98 == p2:
        p98 = p2 + 1

    colors = []
    for val in values:
        if np.isnan(val):
            colors.append([128, 128, 128, 100])  # Gray for missing
        else:
            # Normalize to 0-1, clipping outliers
            norm_val = np.clip((val - p2) / (p98 - p2), 0, 1)
            # Magma colormap: dark -> purple -> magenta -> orange -> light
            rgb = interpolate_magma_color(norm_val)
            colors.append([rgb[0], rgb[1], rgb[2], 180])

    return colors, p2, p98


def colors_to_css(colors: list) -> list[str]:
    """Convert RGBA color arrays to CSS rgba() strings for MapLibre."""
    return [f"rgba({c[0]},{c[1]},{c[2]},{c[3]/255:.2f})" for c in colors]


def swap_coordinates(coords):
    """Recursively swap [lat, lon] to [lon, lat] for GeoJSON compliance."""
    if isinstance(coords[0], list):
        return [swap_coordinates(c) for c in coords]
    return [coords[1], coords[0]]


def swap_coordinates_with_precision(coords, precision=6):
    """Swap [lat, lon] to [lon, lat] and round coordinates to reduce GeoJSON size."""
    if isinstance(coords[0], list):
        return [swap_coordinates_with_precision(c, precision) for c in coords]
    return [round(coords[1], precision), round(coords[0], precision)]


def build_geojson_maplibre(df: pd.DataFrame, metric: str) -> tuple[dict, float, float]:
    """Build GeoJSON optimized for MapLibre with feature IDs and color properties."""
    values = df[metric].values
    colors, p2, p98 = calculate_colors(values)
    css_colors = colors_to_css(colors)  # Convert to CSS strings

    features = []
    for i, (_, row) in enumerate(df.iterrows()):
        try:
            geometry = json.loads(row['geom_4326_geojson'])
            # Swap coordinates from [lat, lon] to [lon, lat] and reduce precision
            geometry['coordinates'] = swap_coordinates_with_precision(
                geometry['coordinates'],
                precision=6
            )
        except (json.JSONDecodeError, TypeError):
            continue

        features.append({
            "type": "Feature",
            "id": i,  # Numeric ID for setFeatureState
            "geometry": geometry,
            "properties": {
                # Identifiers
                "site_parcel_id": row['site_parcel_id'],
                "address": row['parcel_address'] or "N/A",

                # Display values (formatted strings for tooltip)
                "display_total_value": f"${row['current_total_value']:,.0f}" if pd.notna(row['current_total_value']) else "N/A",
                "display_land_value": f"${row['current_land_value']:,.0f}" if pd.notna(row['current_land_value']) else "N/A",
                "display_lot_size": f"{row['lot_size']:,.0f} sq ft" if pd.notna(row['lot_size']) else "N/A",
                "display_net_taxes": f"${row['net_taxes']:,.0f}" if pd.notna(row['net_taxes']) else "N/A",

                # Raw values (numbers for comparison)
                "total_value": float(row['current_total_value']) if pd.notna(row['current_total_value']) else 0,
                "land_value": float(row['current_land_value']) if pd.notna(row['current_land_value']) else 0,
                "lot_size": float(row['lot_size']) if pd.notna(row['lot_size']) else 0,
                "net_taxes": float(row['net_taxes']) if pd.notna(row['net_taxes']) else 0,
                "net_taxes_per_sqft": float(row['net_taxes_per_sqft_lot']) if pd.notna(row['net_taxes_per_sqft_lot']) else 0,
                "land_value_per_sqft": float(row['land_value_per_sqft_lot']) if pd.notna(row['land_value_per_sqft_lot']) else 0,
                "alignment_index": float(row['land_value_alignment_index']) if pd.notna(row['land_value_alignment_index']) else 0,

                # Pre-computed color (CSS rgba string)
                "fillColor": css_colors[i]
            }
        })

    return {"type": "FeatureCollection", "features": features}, p2, p98


def build_geojson(df: pd.DataFrame, metric: str) -> dict:
    """Build GeoJSON FeatureCollection with colors based on selected metric."""
    values = df[metric].values
    colors, p2, p98 = calculate_colors(values)

    features = []
    for i, (_, row) in enumerate(df.iterrows()):
        try:
            geometry = json.loads(row['geom_4326_geojson'])
            # Swap coordinates from [lat, lon] to [lon, lat]
            geometry['coordinates'] = swap_coordinates(geometry['coordinates'])
        except (json.JSONDecodeError, TypeError):
            continue

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "site_parcel_id": row['site_parcel_id'],
                "parcel_address": row['parcel_address'] or "N/A",
                "current_total_value": f"{row['current_total_value']:,.0f}" if pd.notna(row['current_total_value']) else "N/A",
                "current_land_value": f"{row['current_land_value']:,.0f}" if pd.notna(row['current_land_value']) else "N/A",
                "current_improvement_value": f"{row['current_improvement_value']:,.0f}" if pd.notna(row['current_improvement_value']) else "N/A",
                "lot_size": f"{row['lot_size']:,.0f}" if pd.notna(row['lot_size']) else "N/A",
                "net_taxes": f"{row['net_taxes']:,.0f}" if pd.notna(row['net_taxes']) else "N/A",
                "net_taxes_per_sqft_lot": f"{row['net_taxes_per_sqft_lot']:.2f}" if pd.notna(row['net_taxes_per_sqft_lot']) else "N/A",
                "land_value_per_sqft_lot": f"{row['land_value_per_sqft_lot']:.2f}" if pd.notna(row['land_value_per_sqft_lot']) else "N/A",
                "land_value_alignment_index": f"{row['land_value_alignment_index']:.2f}" if pd.notna(row['land_value_alignment_index']) else "N/A",
                "color": colors[i],
            }
        })

    return {"type": "FeatureCollection", "features": features}, p2, p98


# Sidebar
with st.sidebar:
    st.title("Parcel Map")

    selected_metric_label = st.selectbox(
        "Select Metric",
        options=list(METRICS.keys()),
    )
    selected_metric = METRICS[selected_metric_label]

    # Legend
    st.markdown("### Legend")
    st.markdown(f"**{selected_metric_label}**")
    st.markdown("""
    <div style="background: linear-gradient(to right, #FCFDBF, #FC8F59, #B73779, #521C6C, #000004); height: 20px; width: 100%; border-radius: 4px;"></div>
    <div style="display: flex; justify-content: space-between; font-size: 12px;">
        <span>Low</span><span>High</span>
    </div>
    """, unsafe_allow_html=True)

    

# Main content
with st.status("Loading map data...", expanded=True) as status:
    st.write("📊 Querying parcel database...")
    df = load_map_data()
    st.write(f"✅ Loaded {len(df):,} parcels from database")

    if not df.empty:
        st.write("🗺️ Building GeoJSON with colors...")
        geojson_data, p2, p98 = build_geojson_maplibre(df, selected_metric)

        # Calculate GeoJSON size
        import sys
        geojson_size_mb = sys.getsizeof(str(geojson_data)) / (1024 * 1024)
        st.write(f"✅ Generated GeoJSON: {len(geojson_data['features']):,} features ({geojson_size_mb:.1f} MB)")
        st.write("📡 Transferring to browser...")

    status.update(label="Map data ready!", state="complete")

if df.empty:
    st.warning("No parcel data available. Please check the data source.")
else:

    # Show metric range in sidebar
    with st.sidebar:
        if selected_metric == "land_value_alignment_index":
            st.caption(f"Range: {p2:.2f} - {p98:.2f}")
        else:
            st.caption(f"Range: ${p2:.2f} - ${p98:.2f}")
        st.caption(f"Showing {len(geojson_data['features']):,} parcels")
        st.info("Map may take a minute to load.")

    # Render MapLibre component (v2: no height parameter, controlled by CSS)
    from components.maplibre_parcel_map import render_maplibre_map

    component_value = render_maplibre_map(
        geojson_data=geojson_data,
        center=[43.0731, -89.4012],  # Madison, WI [lat, lon]
        zoom=11
    )

    # Store selected parcels in session state
    if component_value and component_value.get('selected_features'):
        st.session_state.map_selected_parcels = component_value['selected_features']
    else:
        st.session_state.map_selected_parcels = []

    # Comparison panel (fragment - only reruns when button clicked)
    st.markdown("---")

    @st.fragment
    def comparison_panel():
        """Render comparison panel for selected parcels."""
        selected = st.session_state.get('map_selected_parcels', [])

        if len(selected) == 0:
            st.info("👆 Click parcels on the map to select them for comparison (max 2)")
            return

        if len(selected) == 1:
            st.info(f"✓ Selected 1 parcel. Select one more to compare.")
            show_single_parcel_summary(selected[0])
            return

        # Show compare button when 2 parcels selected
        if st.button("📊 Compare Selected Parcels", type="primary", use_container_width=True):
            show_parcel_comparison(selected[0], selected[1])

    def show_single_parcel_summary(parcel: dict):
        """Show summary for a single selected parcel."""
        st.markdown(f"### {parcel['address']}")
        st.caption(f"Parcel ID: {parcel['id']}")

        col1, col2, col3 = st.columns(3)

        props = parcel['properties']

        with col1:
            st.metric("Total Value", format_currency(props['total_value']))
            st.metric("Lot Size", f"{format_number(props['lot_size'])} sq ft")

        with col2:
            st.metric("Net Taxes", format_currency(props['net_taxes']))
            st.metric("Net Taxes/sqft", f"${props['net_taxes_per_sqft']:.2f}")

        with col3:
            st.metric("Land Value/sqft", f"${props['land_value_per_sqft']:.2f}")
            st.metric("Alignment Index", f"{props['alignment_index']:.2f}")

    def show_parcel_comparison(parcel1: dict, parcel2: dict):
        """Show side-by-side comparison of two parcels."""
        st.markdown("### 📊 Parcel Comparison")

        # Header row
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"#### {parcel1['address']}")
            st.caption(f"ID: {parcel1['id']}")
        with col2:
            st.markdown(f"#### {parcel2['address']}")
            st.caption(f"ID: {parcel2['id']}")

        st.markdown("---")

        # Metrics comparison
        props1 = parcel1['properties']
        props2 = parcel2['properties']

        metrics = [
            ("Total Assessed Value", "total_value", True),
            ("Land Value", "land_value", True),
            ("Lot Size", "lot_size", False),
            ("Net Taxes", "net_taxes", True),
            ("Net Taxes per sqft", "net_taxes_per_sqft", False),
            ("Land Value per sqft", "land_value_per_sqft", False),
            ("Alignment Index", "alignment_index", False),
        ]

        for metric_name, metric_key, is_currency in metrics:
            col1, col2 = st.columns(2)

            val1 = props1.get(metric_key, 0)
            val2 = props2.get(metric_key, 0)

            with col1:
                if is_currency:
                    st.metric(metric_name, format_currency(val1))
                elif metric_key == "lot_size":
                    st.metric(metric_name, f"{format_number(val1)} sq ft")
                else:
                    st.metric(metric_name, f"{val1:.2f}")

            with col2:
                delta = val2 - val1 if val1 and val2 else None
                if is_currency:
                    st.metric(
                        metric_name,
                        format_currency(val2),
                        delta=format_currency(delta) if delta else None
                    )
                elif metric_key == "lot_size":
                    st.metric(
                        metric_name,
                        f"{format_number(val2)} sq ft",
                        delta=f"{format_number(delta)} sq ft" if delta else None
                    )
                else:
                    st.metric(
                        metric_name,
                        f"{val2:.2f}",
                        delta=f"{delta:+.2f}" if delta else None
                    )

    comparison_panel()
