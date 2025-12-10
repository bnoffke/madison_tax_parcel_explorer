import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
import altair as alt
from streamlit_searchbox import st_searchbox

st.set_page_config(layout="wide", page_title="Madison Tax Parcel Explorer")

# Reduce top padding
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 0rem;
            padding-left: 5rem;
            padding-right: 5rem;
        }
    </style>
    """, unsafe_allow_html=True)

conn = duckdb.connect()

# Load extensions
conn.execute("""
             INSTALL httpfs;
             LOAD httpfs;
             INSTALL spatial;
             LOAD spatial;
             """)

# Create GCS secret
conn.execute(f"""
             CREATE SECRET gcs_secret (
             TYPE gcs,
             KEY_ID '{st.secrets["gcs"]['key_id']}',
             SECRET '{st.secrets["gcs"]['secret']}'
             );
             """)

SILVER_BUCKET = st.secrets["gcs"]['silver_bucket']
GOLD_BUCKET = st.secrets["gcs"]['gold_bucket']

def search_addresses(searchterm: str) -> list[tuple[str, str]]:
    """
    Search for addresses matching the search term using DuckDB lazy loading.

    Args:
        searchterm: User's search input

    Returns:
        List of (display_address, parcel_id) tuples
    """
    # Minimum 2 characters to search
    if not searchterm or len(searchterm) < 2:
        return []

    # Sanitize input to prevent SQL injection
    searchterm = searchterm.replace("'", "''")

    query = f"""
    SELECT
        TRIM(
            CONCAT(
                CAST(house_nbr AS VARCHAR),
                CASE WHEN street_dir IS NOT NULL AND street_dir != '' THEN ' ' || street_dir ELSE '' END,
                ' ', street_name,
                CASE WHEN street_type IS NOT NULL AND street_type != '' THEN ' ' || street_type ELSE '' END,
                CASE WHEN unit IS NOT NULL AND unit != '' THEN ' Unit ' || CAST(unit AS VARCHAR) ELSE '' END
            )
        ) AS full_address,
        parcel_id
    FROM 'gs://{SILVER_BUCKET}/fact_parcels.parquet'
    WHERE CONCAT(
        CAST(house_nbr AS VARCHAR),
        CASE WHEN street_dir IS NOT NULL AND street_dir != '' THEN ' ' || street_dir ELSE '' END,
        ' ', street_name,
        CASE WHEN street_type IS NOT NULL AND street_type != '' THEN ' ' || street_type ELSE '' END,
        CASE WHEN unit IS NOT NULL AND unit != '' THEN ' Unit ' || CAST(unit AS VARCHAR) ELSE '' END
    ) ILIKE '%{searchterm}%'
    ORDER BY
        CASE
            WHEN CONCAT(
                CAST(house_nbr AS VARCHAR),
                CASE WHEN street_dir IS NOT NULL AND street_dir != '' THEN ' ' || street_dir ELSE '' END,
                ' ', street_name,
                CASE WHEN street_type IS NOT NULL AND street_type != '' THEN ' ' || street_type ELSE '' END,
                CASE WHEN unit IS NOT NULL AND unit != '' THEN ' Unit ' || CAST(unit AS VARCHAR) ELSE '' END
            ) ILIKE '{searchterm}%' THEN 1
            ELSE 2
        END,
        house_nbr, street_name
    LIMIT 100
    """

    try:
        results = conn.execute(query).fetchall()
        if not results:
            return [("No addresses found - try a different search", None)]
        return [(addr, pid) for addr, pid in results]
    except Exception as e:
        return [(f"Error searching addresses: {str(e)}", None)]

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_parcel_data(parcel_id: str) -> dict:
    """Load complete parcel data including geometry for selected parcel."""
    if not parcel_id:
        return None

    query = f"""
    SELECT *
    FROM 'gs://{SILVER_BUCKET}/fact_parcels.parquet'
    WHERE parcel_id = '{parcel_id.replace("'", "''")}'
    """

    try:
        result = conn.execute(query).fetchdf()
        return result.to_dict('records')[0] if len(result) > 0 else None
    except Exception as e:
        st.error(f"Error loading parcel data: {str(e)}")
        return None

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_tax_roll_history(parcel_id: str) -> pd.DataFrame:
    """
    Load historical tax roll data for a specific parcel.

    Args:
        parcel_id: The parcel ID to filter by

    Returns:
        DataFrame with columns: tax_year, total_assessed_value, net_tax, effective_tax_rate
        Returns empty DataFrame if no data found or error occurs
    """
    if not parcel_id:
        return pd.DataFrame()

    query = f"""
    SELECT
        tax_year,
        total_assessed_value,
        net_tax
    FROM 'gs://{SILVER_BUCKET}/fact_tax_roll.parquet'
    WHERE parcel_id = '{parcel_id.replace("'", "''")}'
    ORDER BY tax_year
    """

    try:
        result = conn.execute(query).fetchdf()

        if len(result) == 0:
            return pd.DataFrame()

        # Calculate effective_tax_rate with division by zero handling
        result['effective_tax_rate'] = result.apply(
            lambda row: (row['net_tax'] / row['total_assessed_value']) * 100
            if pd.notna(row['total_assessed_value']) and row['total_assessed_value'] > 0
            else None,
            axis=1
        )

        return result

    except Exception as e:
        st.warning(f"Unable to load historical tax data: {str(e)}")
        return pd.DataFrame()

def create_trend_chart(
    df: pd.DataFrame,
    y_column: str,
    y_label: str,
    title: str,
    is_currency: bool = False
) -> alt.Chart:
    """
    Create a standardized Altair line chart for trending data.

    Args:
        df: DataFrame with tax_year and the y-axis column
        y_column: Name of the column to plot on y-axis
        y_label: Label for the y-axis
        title: Chart title
        is_currency: Whether to format as currency (True) or percentage (False)

    Returns:
        Altair Chart object
    """
    # Determine the format for tooltips
    if is_currency:
        format_str = '$,.0f'
        y_axis = alt.Axis(format='$,.0f')
    else:
        format_str = '.2f'
        y_axis = alt.Axis()

    # Create the line chart with points
    chart = alt.Chart(df).mark_line(point=True, color='#1f77b4').encode(
        x=alt.X('tax_year:O', title='Year', axis=alt.Axis(labelAngle=0)),
        y=alt.Y(f'{y_column}:Q', title=y_label, axis=y_axis),
        tooltip=[
            alt.Tooltip('tax_year:O', title='Year'),
            alt.Tooltip(f'{y_column}:Q', title=y_label, format=format_str)
        ]
    ).properties(
        title=title,
        width='container',
        height=250
    ).configure_axis(
        labelFontSize=11,
        titleFontSize=12
    ).configure_title(
        fontSize=14,
        anchor='start'
    )

    return chart

# Helper functions for formatting
def format_currency(value) -> str:
    """Format a numeric value as currency."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        return f"${value:,.0f}"
    except (ValueError, TypeError):
        return "N/A"

def format_percentage(value, decimals=1) -> str:
    """Format a numeric value as percentage."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        return f"{value:.{decimals}f}%"
    except (ValueError, TypeError):
        return "N/A"

def format_number(value, decimals=0) -> str:
    """Format a numeric value with commas."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        if decimals == 0:
            return f"{value:,.0f}"
        else:
            return f"{value:,.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"

def format_tax_change(current_taxes, shift_taxes) -> str:
    """Format tax change with arrow and dollar/percentage."""
    if current_taxes is None or shift_taxes is None or pd.isna(current_taxes) or pd.isna(shift_taxes):
        return "N/A"

    try:
        difference = shift_taxes - current_taxes
        if current_taxes == 0:
            return "N/A (no current taxes)"

        pct_change = (difference / current_taxes) * 100

        if difference > 0:
            arrow = "↑"
            sign = "+"
        elif difference < 0:
            arrow = "↓"
            sign = ""
        else:
            return "No change"

        return f"{arrow} {sign}${abs(difference):,.0f} ({sign}{pct_change:.1f}%)"
    except (ValueError, TypeError, ZeroDivisionError):
        return "N/A"

def format_address(parcel_data: dict) -> str:
    """Format a full address from parcel data."""
    if not parcel_data:
        return "N/A"

    try:
        parts = []

        # House number
        if house_nbr := parcel_data.get('house_nbr'):
            parts.append(str(house_nbr))

        # Street direction
        if street_dir := parcel_data.get('street_dir'):
            if street_dir.strip():
                parts.append(street_dir)

        # Street name
        if street_name := parcel_data.get('street_name'):
            parts.append(street_name)

        # Street type
        if street_type := parcel_data.get('street_type'):
            if street_type.strip():
                parts.append(street_type)

        # Unit
        if unit := parcel_data.get('unit'):
            if str(unit).strip():
                parts.append(f"Unit {unit}")

        return " ".join(parts) if parts else "N/A"
    except (ValueError, TypeError):
        return "N/A"

# App title
st.title("Madison Tax Parcel Explorer")

# Address search on left, selected parcel on right
search_col, status_col = st.columns([1, 1])
with search_col:
    selected_value = st_searchbox(
        search_addresses,
        key="address_search",
        placeholder="Search for an address (e.g., 123 Main St)",
        label="Find a Property",
        debounce=250,  # 250ms delay reduces queries during typing
        clear_on_submit=False,  # Keep address visible after selection
    )

with status_col:
    if selected_value:
        parcel_id = selected_value
        # Skip if user selected the "No addresses found" message
        if parcel_id is None:
            st.info("Please try a different search term.")
        else:
            st.write("")  # Spacer to align with search box label
            msg_col, popover_col = st.columns([3, 1])
            with msg_col:
                # Load parcel data to get the address
                parcel_data_for_display = load_parcel_data(parcel_id)
                if parcel_data_for_display:
                    address = format_address(parcel_data_for_display)
                    st.success(f"**{address}**  (Parcel: {parcel_id})")
                else:
                    st.success(f"Selected parcel: {parcel_id}")
            with popover_col:
                with st.popover("Property Details"):
                    if parcel_data := load_parcel_data(parcel_id):
                        char_col1, char_col2, char_col3 = st.columns(3)

                        with char_col1:
                            st.write(f"**Property Class:** {parcel_data.get('property_class', 'N/A')}")
                            st.write(f"**Property Use:** {parcel_data.get('property_use', 'N/A')}")
                            st.write(f"**Year Built:** {format_number(parcel_data.get('year_built'))}")

                        with char_col2:
                            st.write(f"**Bedrooms:** {format_number(parcel_data.get('bedrooms'))}")
                            st.write(f"**Full Baths:** {format_number(parcel_data.get('full_baths'), decimals=1)}")
                            st.write(f"**Half Baths:** {format_number(parcel_data.get('half_baths'))}")

                        with char_col3:
                            st.write(f"**Total Living Area:** {format_number(parcel_data.get('total_living_area'))} sq ft")
                            st.write(f"**Home Style:** {parcel_data.get('home_style', 'N/A')}")

# Handle parcel data loading and display
if selected_value and selected_value is not None:
    parcel_id = selected_value

    # Load parcel data
    with st.spinner("Loading parcel data..."):
        parcel_data = load_parcel_data(parcel_id)

    if parcel_data:
        # Financial details in 2 columns: data tables on left, charts on right
        data_col, chart_col = st.columns([1, 1])

        with data_col:
            st.markdown("#### Assessment Values")
            assessment_data = pd.DataFrame({
                "Metric": [
                    "Land Value",
                    "Improvement Value",
                    "Total Value",
                    "Land Share of Property",
                    "Lot Size"
                ],
                "Value": [
                    format_currency(parcel_data.get('current_land_value')),
                    format_currency(parcel_data.get('current_improvement_value')),
                    format_currency(parcel_data.get('current_total_value')),
                    format_percentage(parcel_data.get('land_share_property', 0) * 100 if parcel_data.get('land_share_property') else None),
                    f"{format_number(parcel_data.get('lot_size'))} sq ft"
                ]
            })
            st.dataframe(assessment_data, hide_index=True, width='stretch')

            st.markdown("#### Tax Information")
            tax_data = pd.DataFrame({
                "Metric": [
                    "Net Taxes",
                    "Tax Rate",
                    "Net Taxes/sqft",
                ],
                "Value": [
                    format_currency(parcel_data.get('net_taxes')),
                    format_number(parcel_data.get('tax_rate'), decimals=2),
                    f"${format_number(parcel_data.get('net_taxes_per_sqft_lot'), decimals=2)}",
                ]
            })
            st.dataframe(tax_data, hide_index=True, width='stretch')



            st.markdown("#### Land Value Tax Analysis")
            tax_impact = format_tax_change(
                parcel_data.get('net_taxes'),
                parcel_data.get('land_value_shift_taxes')
            )
            lvt_data = pd.DataFrame({
                "Metric": [
                    "Current Net Taxes",
                    "Land Value Shift Taxes",
                    "Tax Impact"
                ],
                "Value": [
                    format_currency(parcel_data.get('net_taxes')),
                    format_currency(parcel_data.get('land_value_shift_taxes')),
                    tax_impact
                ]
            })
            st.dataframe(lvt_data, hide_index=True, width='stretch')

        with chart_col:
            # Load historical tax roll data
            tax_history_df = load_tax_roll_history(parcel_id)

            if not tax_history_df.empty and len(tax_history_df) >= 2:
                st.markdown("#### Historical Trends")
                # Chart 1: Effective Tax Rate
                # Filter out None values for tax rate
                tax_rate_df = tax_history_df[tax_history_df['effective_tax_rate'].notna()]
                if not tax_rate_df.empty:
                    tax_rate_chart = create_trend_chart(
                        tax_rate_df,
                        'effective_tax_rate',
                        'Effective Tax Rate (%)',
                        'Effective Tax Rate Over Time',
                        is_currency=False
                    )
                    st.altair_chart(tax_rate_chart, width='stretch')
                else:
                    st.info("Tax rate data unavailable")

                # Chart 2: Total Assessed Value
                assessed_value_chart = create_trend_chart(
                    tax_history_df,
                    'total_assessed_value',
                    'Assessed Value ($)',
                    'Total Assessed Value Over Time',
                    is_currency=True
                )
                st.altair_chart(assessed_value_chart, width='stretch')


                # Chart 3: Net Taxes
                net_tax_chart = create_trend_chart(
                    tax_history_df,
                    'net_tax',
                    'Net Tax ($)',
                    'Net Taxes Over Time',
                    is_currency=True
                )
                st.altair_chart(net_tax_chart, width='stretch')

            elif not tax_history_df.empty and len(tax_history_df) < 2:
                st.info("At least 2 years of data required to show trends")
            else:
                st.info("No historical data available for this parcel")

        
    else:
        st.error("Parcel data not found.")
