import streamlit as st
import pandas as pd
import pydeck as pdk
import json
import numpy as np

from utils.formatters import format_currency, format_number

# Access shared state
conn = st.session_state.conn
GOLD_BUCKET = st.session_state.GOLD_BUCKET

# Metric configuration
METRICS = {
    "Net Taxes per Sq Ft": "net_taxes_per_sqft_lot",
    "Land Value per Sq Ft": "land_value_per_sqft_lot",
    "Land Value Alignment Index": "land_value_alignment_index",
}


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
            # Yellow [255, 255, 0] (low) -> Purple [128, 0, 128] (high)
            r = int(255 - (255 - 128) * norm_val)
            g = int(255 - 255 * norm_val)
            b = int(0 + 128 * norm_val)
            colors.append([r, g, b, 180])

    return colors, p2, p98


def swap_coordinates(coords):
    """Recursively swap [lat, lon] to [lon, lat] for GeoJSON compliance."""
    if isinstance(coords[0], list):
        return [swap_coordinates(c) for c in coords]
    return [coords[1], coords[0]]


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
    <div style="background: linear-gradient(to right, #FFFF00, #800080); height: 20px; width: 100%; border-radius: 4px;"></div>
    <div style="display: flex; justify-content: space-between; font-size: 12px;">
        <span>Low</span><span>High</span>
    </div>
    """, unsafe_allow_html=True)

    

# Main content
with st.spinner("Loading map data..."):
    df = load_map_data()
    if not df.empty:
        geojson_data, p2, p98 = build_geojson(df, selected_metric)

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

    # Pydeck layer
    layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson_data,
        pickable=True,
        filled=True,
        stroked=True,
        get_fill_color="properties.color",
        get_line_color=[255, 255, 255, 100],
        line_width_min_pixels=0,
    )

    # View state centered on Madison
    view_state = pdk.ViewState(
        latitude=43.0731,
        longitude=-89.4012,
        zoom=11,
        pitch=0,
        bearing=0,
    )

    # Tooltip
    tooltip = {
        "html": """
        <b>{parcel_address}</b><br/>
        <b>Assessed Value:</b> ${current_total_value}<br/>
        <b>Land Value:</b> ${current_land_value}<br/>
        <b>Improvement:</b> ${current_improvement_value}<br/>
        <b>Lot Size:</b> {lot_size} sq ft<br/>
        <b>Net Taxes:</b> ${net_taxes}<br/>
        <hr style="margin: 5px 0;"/>
        <b>Net Taxes/sqft:</b> ${net_taxes_per_sqft_lot}<br/>
        <b>Land Value/sqft:</b> ${land_value_per_sqft_lot}<br/>
        <b>Alignment Index:</b> {land_value_alignment_index}
        """,
        "style": {
            "backgroundColor": "steelblue",
            "color": "white",
            "fontSize": "12px",
            "padding": "10px",
        }
    }

    # Render map
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="light",
        ),
        height=700,
    )
