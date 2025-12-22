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

# Comparison metrics configuration
COMPARISON_METRICS = [
    {"label": "Total Value", "key": "total_value", "type": "currency", "decimals": 0},
    {"label": "Land Value", "key": "land_value", "type": "currency", "decimals": 0},
    {"label": "Lot Size", "key": "lot_size", "type": "area", "decimals": 0},
    {"label": "Net Taxes", "key": "net_taxes", "type": "currency", "decimals": 0},
    {"label": "Net Taxes/sqft", "key": "net_taxes_per_sqft", "type": "currency", "decimals": 2},
    {"label": "Land Value/sqft", "key": "land_value_per_sqft", "type": "currency", "decimals": 2},
    {"label": "Alignment Index", "key": "alignment_index", "type": "number", "decimals": 2}
]

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


def format_metric_value(value, metric_config):
    """
    Format a metric value based on its type.

    Args:
        value: Raw numeric value
        metric_config: Dict with 'type' and 'decimals' keys

    Returns:
        Formatted string
    """
    if value is None or pd.isna(value):
        return "N/A"

    metric_type = metric_config.get('type')
    decimals = metric_config.get('decimals', 0)

    if metric_type == 'currency':
        if decimals == 0:
            return format_currency(value)
        else:
            return f"${value:.{decimals}f}"
    elif metric_type == 'area':
        return f"{format_number(value, decimals=decimals)} sq ft"
    elif metric_type == 'number':
        return format_number(value, decimals=decimals)
    else:
        return str(value)


def calculate_metric_delta(val1, val2, metric_config):
    """
    Calculate and format the difference between two metric values.

    Args:
        val1: First value (baseline)
        val2: Second value
        metric_config: Dict with 'type' and 'decimals' keys

    Returns:
        Formatted delta string with +/- sign
    """
    if val1 is None or val2 is None or pd.isna(val1) or pd.isna(val2):
        return "N/A"

    delta = val2 - val1
    metric_type = metric_config.get('type')
    decimals = metric_config.get('decimals', 0)

    # Sign prefix
    sign = "+" if delta > 0 else ""

    if metric_type == 'currency':
        if decimals == 0:
            return f"{sign}{format_currency(delta)}"
        else:
            return f"{sign}${delta:.{decimals}f}"
    elif metric_type == 'area':
        return f"{sign}{format_number(delta, decimals=decimals)} sq ft"
    elif metric_type == 'number':
        if decimals == 0:
            return f"{sign}{format_number(delta, decimals=decimals)}"
        else:
            return f"{sign}{delta:.{decimals}f}"
    else:
        return f"{sign}{delta}"


def build_comparison_dataframe(parcels: list) -> pd.DataFrame:
    """
    Build a comparison dataframe with metrics as rows and parcels as columns.

    Args:
        parcels: List of parcel dicts (0, 1, or 2 parcels)

    Returns:
        DataFrame with:
        - Index: Metric names
        - Columns: "Parcel 1", "Parcel 2" (if 2 parcels), "Difference" (if 2 parcels)
    """
    num_parcels = len(parcels)

    # Build data dictionary
    data = {"Metric": [m["label"] for m in COMPARISON_METRICS]}

    if num_parcels >= 1:
        # Parcel 1 column
        parcel1_values = []
        for metric in COMPARISON_METRICS:
            value = parcels[0]['properties'].get(metric['key'], None)
            formatted = format_metric_value(value, metric)
            parcel1_values.append(formatted)

        # Use truncated address for column name
        addr1 = parcels[0]['address']
        col1_name = addr1[:30] + "..." if len(addr1) > 30 else addr1
        data[col1_name] = parcel1_values

    if num_parcels == 2:
        # Parcel 2 column
        parcel2_values = []
        for metric in COMPARISON_METRICS:
            value = parcels[1]['properties'].get(metric['key'], None)
            formatted = format_metric_value(value, metric)
            parcel2_values.append(formatted)

        addr2 = parcels[1]['address']
        col2_name = addr2[:30] + "..." if len(addr2) > 30 else addr2
        data[col2_name] = parcel2_values

        # Difference column
        diff_values = []
        for metric in COMPARISON_METRICS:
            val1 = parcels[0]['properties'].get(metric['key'], None)
            val2 = parcels[1]['properties'].get(metric['key'], None)
            delta = calculate_metric_delta(val1, val2, metric)
            diff_values.append(delta)

        data["Difference"] = diff_values

    # Create DataFrame with Metric as index
    df = pd.DataFrame(data)
    df.set_index('Metric', inplace=True)

    return df


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

# Load parcel data and build map visualization
df = load_map_data()

if not df.empty:
    geojson_data, p2, p98 = build_geojson_maplibre(df, selected_metric)

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

    # Import map component
    from components.maplibre_parcel_map import render_maplibre_map

    # Reserve space for button (will be filled after map updates state)
    button_placeholder = st.empty()

    # Render MapLibre component at full width (v2: no height parameter, controlled by CSS)
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

    # NOW render button in placeholder with updated state
    with button_placeholder.container():
        # Comparison popover button
        def comparison_popover():
            """Render comparison popover for selected parcels."""
            selected = st.session_state.get('map_selected_parcels', [])
            num_selected = len(selected)

            # Popover button - always visible
            with st.popover("🏘️Compare Parcels", icon="🏢", help="View comparison of selected parcels",width=600):
                # State 0: No parcels selected
                if num_selected == 0:
                    st.info("👆 Click parcels on the map to compare (max 2)")
                    return

                # State 1: One parcel selected
                if num_selected == 1:
                    df = build_comparison_dataframe(selected)
                    st.dataframe(df)

                    st.info("Select one more parcel to compare")
                    return

                # State 2: Two parcels selected
                df = build_comparison_dataframe(selected)
                st.dataframe(df)

        comparison_popover()
