import streamlit as st
import pandas as pd
import json
import numpy as np

from utils.db import get_connection
from utils.formatters import format_currency, format_number
from components.glossary_dialog import render_glossary_button

# Access shared state (initializes if needed)
conn, _, GOLD_BUCKET = get_connection()

# Metric configuration
METRICS = {
    "Net Taxes per Sq Ft": "net_taxes_per_sqft_lot",
    "Land Value per Sq Ft": "land_value_per_sqft_lot",
    "Land Value Alignment Index": "land_value_alignment_index",
}

# Overlay configuration - defines available map overlay types
OVERLAY_TYPES = {
    "area_plans": {
        "label": "Area Plans",
        "table": "fact_area_plans.parquet",
        "label_field": "area_plan_name",
        "display_name_field": "area_plan_name",
        "comparison_column_prefix": "Area Plan"
    },
    "alder_districts": {
        "label": "Alder Districts",
        "table": "fact_alder_districts.parquet",
        "label_field": "alder_district_name",
        "display_name_field": "alder_district_name",
        "comparison_column_prefix": "District"
    },
    "parcels": {
        "label": "Parcels",
        "table": "fact_sites.parquet",
        "label_field": "parcel_address",
        "display_name_field": "address",
        "comparison_column_prefix": "Parcel"
    }
}

# Default overlay type
DEFAULT_OVERLAY = "area_plans"

# Display order for dropdown
OVERLAY_DISPLAY_ORDER = ["area_plans", "alder_districts", "parcels"]

# Comparison metrics configuration
COMPARISON_METRICS = [
    {"label": "Total Value", "key": "total_value", "type": "currency", "decimals": 0},
    {"label": "Land Value", "key": "land_value", "type": "currency", "decimals": 0},
    {"label": "Lot Size", "key": "lot_size", "type": "area", "decimals": 0},
    {"label": "Net Taxes", "key": "net_taxes", "type": "currency", "decimals": 0},
    {"label": "Net Taxes/sqft", "key": "net_taxes_per_sqft", "type": "currency", "decimals": 2},
    {"label": "Taxes/City Street sqft", "key": "taxes_per_city_street_sqft", "type": "currency", "decimals": 2},
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
def load_map_data(_conn, gold_bucket: str, overlay_type: str) -> pd.DataFrame:
    """Load map data for the selected overlay type.

    Args:
        _conn: DuckDB connection
        gold_bucket: GCS bucket path
        overlay_type: Key from OVERLAY_TYPES dict

    Returns:
        DataFrame with geometry and metrics
    """
    # Validate overlay type
    if overlay_type not in OVERLAY_TYPES:
        st.error(f"Invalid overlay type: {overlay_type}")
        return pd.DataFrame()

    overlay_config = OVERLAY_TYPES[overlay_type]
    label_field = overlay_config["label_field"]
    table = overlay_config["table"]

    # Dynamic SELECT clause and year column based on overlay type
    if overlay_type == "parcels":
        id_field = "site_parcel_id"
        year_column = "parcel_year"
        select_clause = f"""
            {id_field},
            {label_field},
            area_plan_name,
            alder_district_name,
            property_class,
            property_use,
        """
        # Parcels: use current_ prefix columns
        value_columns = """
            current_land_value,
            current_improvement_value,
            current_total_value,
            net_taxes,
            lot_size,
        """
        # Parcels don't have city street metrics
        city_street_columns = ""
        # Additional filters for parcels only
        additional_filters = """
        AND net_taxes > 0
        AND current_total_value > 0
        """
    else:
        # For aggregated overlays (area plans, alder districts)
        year_column = "year_number"
        select_clause = f"""
            {label_field},
        """
        # Aggregated overlays: use total_ prefix columns with aliases to normalize names
        value_columns = """
            total_land_value AS current_land_value,
            total_improvement_value AS current_improvement_value,
            total_value AS current_total_value,
            total_net_taxes AS net_taxes,
            total_area AS lot_size,
        """
        # Aggregated overlays have city street metrics
        city_street_columns = """
            taxes_per_city_maint_street_sqft,
        """
        # No additional filters for aggregated overlays
        additional_filters = ""

    query = f"""
    SELECT
        {select_clause}
        geom_4326_geojson,
        {value_columns}
        {city_street_columns}
        net_taxes_per_sqft_lot,
        land_value_per_sqft_lot,
        land_value_alignment_index
    FROM read_parquet('{gold_bucket}/{table}')
    WHERE {year_column} = (
        SELECT MAX({year_column})
        FROM read_parquet('{gold_bucket}/{table}')
    )
    AND geom_4326_geojson IS NOT NULL
    {additional_filters}
    """

    try:
        df = _conn.execute(query).fetchdf()
        # Add metadata column for validation
        df['overlay_type'] = overlay_type
        return df
    except Exception as e:
        st.error(f"Error loading map data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_parcel_filter_options(_conn, gold_bucket: str):
    """Load distinct combinations with count-based filtering for property_use/property_class."""
    # Get full combinations of all 4 dimensions for filterable values only
    query = f"""
    WITH filterable_props AS (
        SELECT DISTINCT property_class, property_use
        FROM read_parquet('{gold_bucket}/fact_sites.parquet')
        WHERE property_class IS NOT NULL
          AND property_use IS NOT NULL
        GROUP BY ALL
        HAVING COUNT(*) >= 50
    )
    SELECT DISTINCT
        s.area_plan_name,
        s.alder_district_name,
        s.property_class,
        s.property_use
    FROM read_parquet('{gold_bucket}/fact_sites.parquet') s
    INNER JOIN filterable_props fp
        ON s.property_class = fp.property_class
        AND s.property_use = fp.property_use
    WHERE s.area_plan_name IS NOT NULL
      AND s.alder_district_name IS NOT NULL
    ORDER BY s.area_plan_name, s.alder_district_name, s.property_use
    """
    return _conn.execute(query).df()


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


def build_comparison_dataframe(parcels: list, overlay_type: str) -> pd.DataFrame:
    """
    Build a comparison dataframe with metrics as rows and features as columns.

    Args:
        parcels: List of feature dicts (0, 1, or 2 features)
        overlay_type: Current overlay type for column naming

    Returns:
        DataFrame with:
        - Index: Metric names
        - Columns: Feature columns with difference if 2 features
    """
    num_parcels = len(parcels)

    # Validation: Check all parcels are same overlay type
    if num_parcels > 1:
        overlay_types = [p.get('overlay_type') for p in parcels]
        if len(set(overlay_types)) > 1:
            st.warning("⚠️ Cannot compare features from different overlay types. Please select features of the same type.")
            return pd.DataFrame()

    # Filter metrics based on overlay type
    if overlay_type == "parcels":
        # Exclude city street metric for parcels
        metrics_to_show = [m for m in COMPARISON_METRICS if m["key"] != "taxes_per_city_street_sqft"]
    else:
        # Show all metrics for aggregated overlays
        metrics_to_show = COMPARISON_METRICS

    # Build data dictionary
    data = {"Metric": [m["label"] for m in metrics_to_show]}

    if num_parcels >= 1:
        # Feature 1 column
        feature1_values = []
        for metric in metrics_to_show:
            value = parcels[0]['properties'].get(metric['key'], None)
            formatted = format_metric_value(value, metric)
            feature1_values.append(formatted)

        # Use label for column name (truncated if needed)
        label1 = parcels[0]['label']
        col1_name = label1[:30] + "..." if len(label1) > 30 else label1
        data[col1_name] = feature1_values

    if num_parcels == 2:
        # Feature 2 column
        feature2_values = []
        for metric in metrics_to_show:
            value = parcels[1]['properties'].get(metric['key'], None)
            formatted = format_metric_value(value, metric)
            feature2_values.append(formatted)

        label2 = parcels[1]['label']
        col2_name = label2[:30] + "..." if len(label2) > 30 else label2
        data[col2_name] = feature2_values

        # Difference column
        diff_values = []
        for metric in metrics_to_show:
            val1 = parcels[0]['properties'].get(metric['key'], None)
            val2 = parcels[1]['properties'].get(metric['key'], None)
            delta = calculate_metric_delta(val1, val2, metric)
            diff_values.append(delta)

        data["Difference"] = diff_values

    # Create DataFrame with Metric as index
    df = pd.DataFrame(data)
    df.set_index('Metric', inplace=True)

    return df


def render_group_comparison(payload: dict, overlay_type: str) -> None:
    """
    Render comparison UI for group mode.

    Args:
        payload: Dict with comparison_mode, group1, group2 keys
        overlay_type: Current overlay type for metric filtering
    """
    group1 = payload.get('group1')
    group2 = payload.get('group2')

    if not group1 or not group2:
        st.warning("Incomplete group selection. Please confirm both groups before comparing.")
        return

    agg1 = group1.get('aggregate')
    agg2 = group2.get('aggregate')

    if not agg1 or not agg2:
        st.warning("Missing aggregate data for groups.")
        return

    # Filter metrics based on overlay type
    if overlay_type == "parcels":
        metrics_to_show = [m for m in COMPARISON_METRICS if m["key"] != "taxes_per_city_street_sqft"]
    else:
        metrics_to_show = COMPARISON_METRICS

    # Build data dictionary
    data = {"Metric": [m["label"] for m in metrics_to_show]}

    # Group 1 column
    g1_values = []
    for metric in metrics_to_show:
        value = agg1.get(metric['key'], None)
        formatted = format_metric_value(value, metric)
        g1_values.append(formatted)
    data[f"Group 1 (n={agg1.get('count', 0)})"] = g1_values

    # Group 2 column
    g2_values = []
    for metric in metrics_to_show:
        value = agg2.get(metric['key'], None)
        formatted = format_metric_value(value, metric)
        g2_values.append(formatted)
    data[f"Group 2 (n={agg2.get('count', 0)})"] = g2_values

    # Difference column
    diff_values = []
    for metric in metrics_to_show:
        val1 = agg1.get(metric['key'], None)
        val2 = agg2.get(metric['key'], None)
        delta = calculate_metric_delta(val1, val2, metric)
        diff_values.append(delta)
    data["Difference"] = diff_values

    # Create DataFrame
    df = pd.DataFrame(data)
    df.set_index('Metric', inplace=True)

    # Display
    st.dataframe(df, use_container_width=True)

    # Show group details in expandable sections
    with st.expander(f"📋 Group 1 Details ({agg1.get('count', 0)} features)"):
        features1 = group1.get('features', [])
        if features1:
            st.write(", ".join([f.get('label', f.get('id', 'Unknown')) for f in features1]))

    with st.expander(f"📋 Group 2 Details ({agg2.get('count', 0)} features)"):
        features2 = group2.get('features', [])
        if features2:
            st.write(", ".join([f.get('label', f.get('id', 'Unknown')) for f in features2]))


def filter_dataframe(df: pd.DataFrame, overlay_type: str, area_plans: list, alder_districts: list,
                     property_class: str, property_use: str) -> pd.DataFrame:
    """
    Filter DataFrame in-memory based on selected filter values.
    Only applies to parcels overlay.
    """
    if overlay_type != "parcels":
        return df

    # Only filter if at least one filter has selections
    if not any([area_plans, alder_districts, property_class, property_use]):
        return df

    filtered_df = df.copy()

    if area_plans:
        filtered_df = filtered_df[filtered_df['area_plan_name'].isin(area_plans)]

    if alder_districts:
        filtered_df = filtered_df[filtered_df['alder_district_name'].isin(alder_districts)]

    if property_class:
        filtered_df = filtered_df[filtered_df['property_class'] == property_class]

    if property_use:
        filtered_df = filtered_df[filtered_df['property_use'] == property_use]

    return filtered_df


def build_geojson_maplibre(df: pd.DataFrame, metric: str, overlay_type: str) -> tuple[dict, float, float]:
    """Build GeoJSON optimized for MapLibre with feature IDs and color properties.

    Args:
        df: DataFrame with map data
        metric: Metric column to use for coloring
        overlay_type: Key from OVERLAY_TYPES dict

    Returns:
        Tuple of (GeoJSON dict, p2 value, p98 value)
    """
    values = df[metric].values
    colors, p2, p98 = calculate_colors(values)
    css_colors = colors_to_css(colors)  # Convert to CSS strings

    overlay_config = OVERLAY_TYPES[overlay_type]
    label_field = overlay_config["label_field"]
    display_field = overlay_config["display_name_field"]

    features = []
    for i, (_, row) in enumerate(df.iterrows()):
        try:
            geometry = json.loads(row['geom_4326_geojson'])
        except (json.JSONDecodeError, TypeError):
            continue

        # Extract label value dynamically
        label_value = row.get(label_field, "N/A")

        # For parcels, also include site_parcel_id
        if overlay_type == "parcels":
            feature_id_value = row.get('site_parcel_id', label_value)
        else:
            feature_id_value = label_value

        features.append({
            "type": "Feature",
            "id": i,  # Numeric ID for setFeatureState
            "geometry": geometry,
            "properties": {
                # Dynamic identifier fields
                "feature_id": feature_id_value,
                display_field: label_value,
                "overlay_type": overlay_type,

                # Display values (formatted strings for tooltip)
                "display_total_value": f"${row['current_total_value']:,.0f}" if pd.notna(row['current_total_value']) else "N/A",
                "display_land_value": f"${row['current_land_value']:,.0f}" if pd.notna(row['current_land_value']) else "N/A",
                "display_lot_size": f"{row['lot_size']:,.0f} sq ft" if pd.notna(row['lot_size']) else "N/A",
                "display_net_taxes": f"${row['net_taxes']:,.0f}" if pd.notna(row['net_taxes']) else "N/A",

                # Property classification (for parcels only)
                "property_class": row.get('property_class', 'N/A') if overlay_type == "parcels" else None,
                "property_use": row.get('property_use', 'N/A') if overlay_type == "parcels" else None,

                # Raw values (numbers for comparison)
                "total_value": float(row['current_total_value']) if pd.notna(row['current_total_value']) else 0,
                "land_value": float(row['current_land_value']) if pd.notna(row['current_land_value']) else 0,
                "lot_size": float(row['lot_size']) if pd.notna(row['lot_size']) else 0,
                "net_taxes": float(row['net_taxes']) if pd.notna(row['net_taxes']) else 0,
                "net_taxes_per_sqft": float(row['net_taxes_per_sqft_lot']) if pd.notna(row['net_taxes_per_sqft_lot']) else 0,
                "taxes_per_city_street_sqft": float(row['taxes_per_city_maint_street_sqft']) if 'taxes_per_city_maint_street_sqft' in row and pd.notna(row['taxes_per_city_maint_street_sqft']) else 0,
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


def get_filtered_options(df_combinations, selected_areas, selected_districts,
                        selected_class, selected_use):
    """
    Filter options based on current selections across all 4 dimensions.
    Returns (available_areas, available_districts, available_classes, available_uses)
    """
    filtered = df_combinations.copy()

    # Apply each filter if selections exist
    if selected_areas:
        filtered = filtered[filtered['area_plan_name'].isin(selected_areas)]
    if selected_districts:
        filtered = filtered[filtered['alder_district_name'].isin(selected_districts)]
    if selected_class:
        filtered = filtered[filtered['property_class'] == selected_class]
    if selected_use:
        filtered = filtered[filtered['property_use'] == selected_use]

    # Extract unique values for each dimension
    available_areas = sorted(filtered['area_plan_name'].unique().tolist())
    available_districts = sorted(filtered['alder_district_name'].unique().tolist())
    available_classes = sorted(filtered['property_class'].unique().tolist())
    available_uses = sorted(filtered['property_use'].unique().tolist())

    return available_areas, available_districts, available_classes, available_uses


# Initialize session state for overlay type
if 'selected_overlay_type' not in st.session_state:
    st.session_state.selected_overlay_type = DEFAULT_OVERLAY
if 'selected_area_plans' not in st.session_state:
    st.session_state.selected_area_plans = []
if 'selected_alder_districts' not in st.session_state:
    st.session_state.selected_alder_districts = []
if 'selected_property_classes' not in st.session_state:
    st.session_state.selected_property_classes = None
if 'selected_property_uses' not in st.session_state:
    st.session_state.selected_property_uses = None

# Callback to clear selections when overlay changes
def on_overlay_change():
    st.session_state.map_selected_parcels = []
    st.session_state.selected_area_plans = []
    st.session_state.selected_alder_districts = []
    st.session_state.selected_property_classes = None
    st.session_state.selected_property_uses = None

# Sidebar
with st.sidebar:
    st.title("Parcel Map")

    # Overlay type selector
    overlay_type = st.selectbox(
        "Select Overlay Type",
        options=OVERLAY_DISPLAY_ORDER,
        format_func=lambda x: OVERLAY_TYPES[x]["label"],
        index=OVERLAY_DISPLAY_ORDER.index(st.session_state.selected_overlay_type),
        key="overlay_selector",
        on_change=on_overlay_change
    )

    # Update session state
    st.session_state.selected_overlay_type = overlay_type

    # Metric selector
    selected_metric_label = st.selectbox(
        "Select Metric",
        options=list(METRICS.keys()),
    )
    selected_metric = METRICS[selected_metric_label]

    # Parcel-specific filters (only shown when parcels overlay is selected)
    if overlay_type == "parcels":
        # Load filter options
        df_filter_options = load_parcel_filter_options(conn, GOLD_BUCKET)

        # Get currently available options based on selections
        available_areas, available_districts, available_classes, available_uses = get_filtered_options(
            df_filter_options,
            st.session_state.selected_area_plans,
            st.session_state.selected_alder_districts,
            st.session_state.selected_property_classes,  # Single value (or None)
            st.session_state.selected_property_uses      # Single value (or None)
        )

        # Area plan filter
        selected_area_plans = st.multiselect(
            "Area Plans",
            options=available_areas,
            default=st.session_state.selected_area_plans,
            key="area_plan_filter"
        )
        st.session_state.selected_area_plans = selected_area_plans

        # Alder district filter
        selected_alder_districts = st.multiselect(
            "Alder Districts",
            options=available_districts,
            default=st.session_state.selected_alder_districts,
            key="alder_district_filter"
        )
        st.session_state.selected_alder_districts = selected_alder_districts

        # Property class filter
        class_options = ["All"] + available_classes
        class_index = 0
        if st.session_state.selected_property_classes and st.session_state.selected_property_classes in available_classes:
            class_index = class_options.index(st.session_state.selected_property_classes)

        selected_property_class_raw = st.selectbox(
            "Property Class",
            options=class_options,
            index=class_index,
            key="property_class_filter"
        )
        selected_property_class = selected_property_class_raw if selected_property_class_raw != "All" else None
        st.session_state.selected_property_classes = selected_property_class

        # Property use filter
        use_options = ["All"] + available_uses
        use_index = 0
        if st.session_state.selected_property_uses and st.session_state.selected_property_uses in available_uses:
            use_index = use_options.index(st.session_state.selected_property_uses)

        selected_property_use_raw = st.selectbox(
            "Property Use",
            options=use_options,
            index=use_index,
            key="property_use_filter"
        )
        selected_property_use = selected_property_use_raw if selected_property_use_raw != "All" else None
        st.session_state.selected_property_uses = selected_property_use
    else:
        # Clear filters when not on parcels overlay
        selected_area_plans = []
        selected_alder_districts = []
        selected_property_class = None
        selected_property_use = None

    # Legend
    st.markdown("### Legend")
    st.markdown(f"**{selected_metric_label}**")
    st.markdown("""
    <div style="background: linear-gradient(to right, #FCFDBF, #FC8F59, #B73779, #521C6C, #000004); height: 20px; width: 100%; border-radius: 4px;"></div>
    <div style="display: flex; justify-content: space-between; font-size: 12px;">
        <span>Low</span><span>High</span>
    </div>
    """, unsafe_allow_html=True)

# Load data (cached by overlay type only)
df = load_map_data(conn, GOLD_BUCKET, overlay_type)

# Apply in-memory filtering for parcels
if overlay_type == "parcels":
    df = filter_dataframe(df, overlay_type, selected_area_plans, selected_alder_districts,
                         selected_property_class, selected_property_use)

if not df.empty:
    geojson_data, p2, p98 = build_geojson_maplibre(df, selected_metric, overlay_type)

if df.empty:
    st.warning("No data available. Please check the data source.")
else:

    # Show metric range in sidebar
    with st.sidebar:
        if selected_metric == "land_value_alignment_index":
            st.caption(f"Range: {p2:.2f} - {p98:.2f}")
        else:
            st.caption(f"Range: ${p2:.2f} - ${p98:.2f}")

        # Update caption based on overlay type
        overlay_label = OVERLAY_TYPES[overlay_type]["label"]
        feature_count = len(geojson_data['features'])
        st.caption(f"Showing {feature_count:,} {overlay_label.lower()}")
        st.info("Map may take a minute to load.")

        # Glossary
        st.markdown("---")
        render_glossary_button()

    # Import map component
    from components.maplibre_parcel_map import render_maplibre_map

    # Reserve space for button (will be filled after map updates state)
    button_placeholder = st.empty()

    # Render MapLibre component with overlay config
    overlay_config = {
        "display_name_field": OVERLAY_TYPES[overlay_type]["display_name_field"],
        "overlay_type": overlay_type
    }
    component_value = render_maplibre_map(
        geojson_data=geojson_data,
        center=[43.0731, -89.4012],  # Madison, WI [lat, lon]
        zoom=11,
        overlay_config=overlay_config
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
            """Render comparison popover for selected features."""
            selected = st.session_state.get('map_selected_parcels', [])

            # Update popover label based on overlay type
            overlay_label = OVERLAY_TYPES[overlay_type]["label"]

            # Check for group mode payload
            if isinstance(selected, dict) and selected.get('comparison_mode') == 'group':
                popover_label = f"Compare {overlay_label} Groups"
                with st.popover(f"📊 {popover_label}", icon="🏢", help="View group comparison", use_container_width=True):
                    render_group_comparison(selected, overlay_type)
                return

            # Individual mode (existing logic)
            num_selected = len(selected) if selected else 0
            popover_label = f"Compare {overlay_label}"

            # Popover button - always visible
            with st.popover(f"🏘️ {popover_label}", icon="🏢", help=f"View comparison of selected {overlay_label.lower()}", use_container_width=True):
                # State 0: No features selected
                if num_selected == 0:
                    st.info(f"👆 Click {overlay_label.lower()} on the map to compare (max 2)")
                    return

                # State 1: One feature selected
                if num_selected == 1:
                    df = build_comparison_dataframe(selected, overlay_type)
                    if not df.empty:
                        st.dataframe(df)
                    st.info(f"Select one more {overlay_label.lower()} to compare")
                    return

                # State 2: Two features selected
                df = build_comparison_dataframe(selected, overlay_type)
                if not df.empty:
                    st.dataframe(df)

        comparison_popover()
