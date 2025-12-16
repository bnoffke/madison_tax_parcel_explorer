import streamlit as st
import pandas as pd
import altair as alt
from streamlit_searchbox import st_searchbox
from rapidfuzz import process, fuzz

from utils.db import get_connection
from utils.formatters import (
    format_currency,
    format_percentage,
    format_number,
    format_address,
)

st.set_page_config(layout="wide")

# Access shared state (initializes if needed)
conn, SILVER_BUCKET, GOLD_BUCKET = get_connection()

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


def search_addresses(searchterm: str) -> list[tuple[str, str]]:
    """
    Search for addresses using in-memory table with fuzzy matching fallback.

    Args:
        searchterm: User's search input

    Returns:
        List of (display_address, parcel_id) tuples
    """
    # Minimum 2 characters to search
    if not searchterm or len(searchterm) < 2:
        return []

    # Sanitize input to prevent SQL injection
    searchterm_clean = searchterm.replace("'", "''")

    # Check if in-memory search is enabled
    if st.session_state.get('address_search_enabled', False):
        try:
            # Phase 1: Exact substring search from in-memory table
            exact_query = f"""
            SELECT full_address, parcel_id
            FROM parcel_addresses
            WHERE full_address ILIKE '%{searchterm_clean}%'
            ORDER BY
                CASE WHEN full_address ILIKE '{searchterm_clean}%' THEN 1 ELSE 2 END,
                full_address
            LIMIT 100
            """
            exact_results = conn.execute(exact_query).fetchall()

            # Phase 2: Fuzzy search if exact results are sparse
            if len(exact_results) < 10:
                # Get all addresses for fuzzy matching
                all_addresses = conn.execute(
                    "SELECT full_address, parcel_id FROM parcel_addresses"
                ).fetchall()

                # Use RapidFuzz for fuzzy matching
                fuzzy_matches = process.extract(
                    searchterm,
                    [(addr, pid) for addr, pid in all_addresses],
                    scorer=fuzz.token_set_ratio,
                    limit=20,
                    score_cutoff=70,
                    processor=lambda x: x[0]  # Compare against full_address
                )

                # Merge results, avoiding duplicates
                seen_parcels = {pid for _, pid in exact_results}
                for match, score, _ in fuzzy_matches:
                    addr, pid = match
                    if pid not in seen_parcels:
                        exact_results.append((addr, pid))
                        seen_parcels.add(pid)

            if not exact_results:
                return [("No addresses found - try a different search", None)]

            return [(addr, pid) for addr, pid in exact_results[:100]]

        except Exception as e:
            st.warning(f"In-memory search failed: {e}. Falling back to direct query.")
            # Fall through to fallback mode

    # Fallback: Query GCS parquet directly
    fallback_query = f"""
    SELECT
        full_address,
        parcel_id
    FROM read_parquet('{SILVER_BUCKET}/fact_parcels.parquet')
    WHERE full_address ILIKE '%{searchterm_clean}%'
    ORDER BY
        CASE WHEN full_address ILIKE '{searchterm_clean}%' THEN 1 ELSE 2 END,
        full_address
    LIMIT 100
    """

    try:
        results = conn.execute(fallback_query).fetchall()
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
    FROM read_parquet('{SILVER_BUCKET}/fact_parcels.parquet')
    WHERE parcel_id = '{parcel_id.replace("'", "''")}'
    """

    try:
        result = conn.execute(query).fetchdf()
        return result.to_dict('records')[0] if len(result) > 0 else None
    except Exception as e:
        st.error(f"Error loading parcel data: {str(e)}")
        return None

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_site_data(site_parcel_id: str) -> dict:
    """Load site data for metrics display when parcel differs from site."""
    if not site_parcel_id:
        return None

    query = f"""
    SELECT
        net_taxes_per_sqft_lot,
        land_value_per_sqft_lot,
        land_value_alignment_index,
        current_land_value,
        current_total_value
    FROM read_parquet('{GOLD_BUCKET}/fact_sites.parquet')
    WHERE site_parcel_id = '{site_parcel_id.replace("'", "''")}'
    AND parcel_year = (
        SELECT MAX(parcel_year)
        FROM read_parquet('{GOLD_BUCKET}/fact_sites.parquet')
    )
    """

    try:
        result = conn.execute(query).fetchdf()
        if len(result) > 0:
            site = result.to_dict('records')[0]
            # Calculate land_share_property if not directly available
            if site.get('current_total_value') and site['current_total_value'] > 0:
                site['land_share_property'] = site['current_land_value'] / site['current_total_value']
            else:
                site['land_share_property'] = None
            return site
        return None
    except Exception as e:
        st.warning(f"Unable to load site data: {str(e)}")
        return None


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_tax_roll_history(parcel_id: str) -> pd.DataFrame:
    """
    Load historical tax roll data for a specific parcel.

    Args:
        parcel_id: The parcel ID to filter by

    Returns:
        DataFrame with columns: tax_year, total_assessed_value, net_tax,
        city_tax, county_tax, school_tax, matc_tax, effective_tax_rate
        Returns empty DataFrame if no data found or error occurs
    """
    if not parcel_id:
        return pd.DataFrame()

    query = f"""
    SELECT
        tax_year,
        total_assessed_value,
        assessed_value_land,
        assessed_value_improvement,
        net_tax,
        city_tax,
        county_tax,
        school_tax,
        matc_tax
    FROM read_parquet('{SILVER_BUCKET}/fact_tax_roll.parquet')
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


def create_assessed_value_trend_chart(df: pd.DataFrame) -> alt.Chart:
    """
    Create a multi-line chart showing total, land, and improvement assessed values over time.

    Args:
        df: DataFrame with tax_year, total_assessed_value, assessed_value_land, assessed_value_improvement

    Returns:
        Altair Chart object with three trend lines
    """
    # Melt the dataframe to long format for multi-line chart
    value_columns = ['total_assessed_value', 'assessed_value_land', 'assessed_value_improvement']
    df_melted = df.melt(
        id_vars=['tax_year'],
        value_vars=value_columns,
        var_name='value_type',
        value_name='amount'
    )

    # Clean up value type names for display
    type_labels = {
        'total_assessed_value': 'Total',
        'assessed_value_land': 'Land',
        'assessed_value_improvement': 'Improvement'
    }
    df_melted['value_type'] = df_melted['value_type'].map(type_labels)

    # Color scale for value types
    color_scale = alt.Scale(
        domain=['Total', 'Land', 'Improvement'],
        range=['#1f77b4', '#2ca02c', '#ff7f0e']
    )

    chart = alt.Chart(df_melted).mark_line(point=True).encode(
        x=alt.X('tax_year:O', title='Year', axis=alt.Axis(labelAngle=0)),
        y=alt.Y('amount:Q', title='Value ($)', axis=alt.Axis(format='$,.0f')),
        color=alt.Color('value_type:N', title=None, scale=color_scale),
        tooltip=[
            alt.Tooltip('tax_year:O', title='Year'),
            alt.Tooltip('value_type:N', title='Type'),
            alt.Tooltip('amount:Q', title='Value', format='$,.0f')
        ]
    ).properties(
        title='Assessed Value',
        width='container',
        height=250
    ).configure_axis(
        labelFontSize=11,
        titleFontSize=12
    ).configure_title(
        fontSize=14,
        anchor='start'
    ).configure_legend(
        orient='none',
        legendX=130,
        legendY=-50,
        direction='horizontal',
        labelFontSize=10
    )

    return chart


def create_tax_sources_chart(df: pd.DataFrame, group_by: str = "source") -> alt.Chart:
    """
    Create a grouped bar chart showing tax breakdown by source over time.

    Args:
        df: DataFrame with tax_year, city_tax, county_tax, school_tax, matc_tax
        group_by: "source" to group by tax source (see yearly trends per source),
                  "year" to group by year (see source breakdown per year)

    Returns:
        Altair Chart object
    """
    # Melt the dataframe to long format for grouped bar chart
    tax_columns = ['city_tax', 'county_tax', 'school_tax', 'matc_tax']
    df_melted = df.melt(
        id_vars=['tax_year'],
        value_vars=tax_columns,
        var_name='tax_source',
        value_name='amount'
    )

    # Clean up tax source names for display
    source_labels = {
        'city_tax': 'City',
        'county_tax': 'County',
        'school_tax': 'School',
        'matc_tax': 'MATC'
    }
    df_melted['tax_source'] = df_melted['tax_source'].map(source_labels)

    # Color scale for tax sources (consistent across both views)
    source_color_scale = alt.Scale(
        domain=['City', 'School', 'County', 'MATC'],
        range=['#1f77b4', '#2ca02c', '#ff7f0e', '#d62728']
    )

    # Configure encoding based on grouping mode
    if group_by == "source":
        # Group by source: x-axis is source, bars within each group are years
        x_encoding = alt.X(
            'tax_source:N',
            title='Tax Source',
            sort=['City', 'School', 'County', 'MATC'],
            axis=alt.Axis(labelAngle=0)
        )
        x_offset = 'tax_year:O'
        color_encoding = alt.Color(
            'tax_source:N',
            title='Tax Source',
            scale=source_color_scale
        )
        chart_title = 'Tax Trends by Source'
    else:
        # Group by year: x-axis is year, bars within each group are sources
        x_encoding = alt.X('tax_year:O', title='Year', axis=alt.Axis(labelAngle=0))
        x_offset = 'tax_source:N'
        color_encoding = alt.Color(
            'tax_source:N',
            title='Tax Source',
            scale=source_color_scale
        )
        chart_title = 'Tax Breakdown by Year'

    chart = alt.Chart(df_melted).mark_bar().encode(
        x=x_encoding,
        y=alt.Y('amount:Q', title='Tax Amount ($)', axis=alt.Axis(format='$,.0f')),
        color=color_encoding,
        xOffset=x_offset,
        tooltip=[
            alt.Tooltip('tax_year:O', title='Year'),
            alt.Tooltip('tax_source:N', title='Source'),
            alt.Tooltip('amount:Q', title='Amount', format='$,.0f')
        ]
    ).properties(
        title=chart_title,
        width='container',
        height=300
    ).configure_axis(
        labelFontSize=11,
        titleFontSize=12
    ).configure_title(
        fontSize=14,
        anchor='start'
    ).configure_legend(
        orient='bottom',
        titleFontSize=11,
        labelFontSize=10
    )

    return chart


# App title
st.title("Parcel Explorer")

# Show loaded parcel count if in-memory search is enabled
if st.session_state.get('address_search_enabled'):
    try:
        parcel_count = conn.execute("SELECT COUNT(*) FROM parcel_addresses").fetchone()[0]
        st.caption(f"📍 {parcel_count:,} parcels loaded")
    except Exception:
        pass  # Silently skip if count fails

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
                            st.write(f"**Property Use:**  \n{parcel_data.get('property_use', 'N/A')}")
                            st.write(f"**Year Built:** {format_number(parcel_data.get('year_built'))}")

                        with char_col2:
                            st.write(f"**Bedrooms:** {format_number(parcel_data.get('bedrooms'))}")
                            st.write(f"**Full Baths:** {format_number(parcel_data.get('full_baths'))}")
                            st.write(f"**Half Baths:** {format_number(parcel_data.get('half_baths'))}")

                        with char_col3:
                            st.write(f"**Total Living Area:**  \n{format_number(parcel_data.get('total_living_area'))} sq ft")
                            st.write(f"**Home Style:** {parcel_data.get('home_style', 'N/A')}")

# Handle parcel data loading and display
if selected_value and selected_value is not None:
    parcel_id = selected_value

    # Load parcel data
    with st.spinner("Loading parcel data..."):
        parcel_data = load_parcel_data(parcel_id)

    if parcel_data:
        # Layout: slim left column (20%) for tables, wide right column (80%) for charts
        left_col, right_col = st.columns([0.2, 0.8])

        with left_col:
            st.markdown("#### Assessments")
            assessment_data = pd.DataFrame({
                "Metric": [
                    "Land Value",
                    "Improvement Value",
                    "Total Assessed Value",
                    "Net Taxes",
                    "Lot Size"
                ],
                "Value": [
                    format_currency(parcel_data.get('current_land_value')),
                    format_currency(parcel_data.get('current_improvement_value')),
                    format_currency(parcel_data.get('current_total_value')),
                    format_currency(parcel_data.get('net_taxes')),
                    f"{format_number(parcel_data.get('lot_size'))} sq ft"
                ]
            })
            st.dataframe(assessment_data, hide_index=True, width='stretch')

            st.markdown("#### Land Efficiency")

            # Check if parcel differs from site
            parcel_id_val = parcel_data.get('parcel_id')
            site_parcel_id = parcel_data.get('site_parcel_id')
            use_site_metrics = parcel_id_val and site_parcel_id and parcel_id_val != site_parcel_id
            parcel_land_value_zero = (parcel_data.get('current_land_value') or 0) == 0

            # Load site data if needed
            site_data = None
            if use_site_metrics:
                site_data = load_site_data(site_parcel_id)

            # Build metrics with conditional site values
            if use_site_metrics and site_data:
                land_value_label = "Land Value per sqft (site)"
                land_value_val = f"${format_number(site_data.get('land_value_per_sqft_lot'), decimals=2)}"
                taxes_label = "Net Taxes per sqft (site)"
                taxes_val = f"${format_number(site_data.get('net_taxes_per_sqft_lot'), decimals=2)}"

                if parcel_land_value_zero:
                    land_share_label = "Land Share of Property (site)"
                    land_share_val = format_percentage(site_data.get('land_share_property', 0) * 100 if site_data.get('land_share_property') else None)
                    alignment_label = "Land Value Alignment Index (site)"
                    alignment_val = format_number(site_data.get('land_value_alignment_index'), decimals=2)
                else:
                    land_share_label = "Land Share of Property"
                    land_share_val = format_percentage(parcel_data.get('land_share_property', 0) * 100 if parcel_data.get('land_share_property') else None)
                    alignment_label = "Land Value Alignment Index"
                    alignment_val = format_number(parcel_data.get('land_value_alignment_index'), decimals=2)
            else:
                land_value_label = "Land Value per sqft"
                land_value_val = f"${format_number(parcel_data.get('land_value_per_sqft_lot'), decimals=2)}"
                taxes_label = "Net Taxes per sqft"
                taxes_val = f"${format_number(parcel_data.get('net_taxes_per_sqft_lot'), decimals=2)}"
                land_share_label = "Land Share of Property"
                land_share_val = format_percentage(parcel_data.get('land_share_property', 0) * 100 if parcel_data.get('land_share_property') else None)
                alignment_label = "Land Value Alignment Index"
                alignment_val = format_number(parcel_data.get('land_value_alignment_index'), decimals=2)

            efficiency_data = pd.DataFrame({
                "Metric": [land_value_label, taxes_label, land_share_label, alignment_label],
                "Value": [land_value_val, taxes_val, land_share_val, alignment_val]
            })
            st.dataframe(efficiency_data, hide_index=True, width='stretch')

        with right_col:
            # Load historical tax roll data
            tax_history_df = load_tax_roll_history(parcel_id)

            if not tax_history_df.empty and len(tax_history_df) >= 2:
                st.markdown("#### Historical Trends")

                # Top row: 3 trend charts side by side
                chart1_col, chart2_col, chart3_col = st.columns(3)

                with chart1_col:
                    # Chart 1: Effective Tax Rate
                    tax_rate_df = tax_history_df[tax_history_df['effective_tax_rate'].notna()]
                    if not tax_rate_df.empty:
                        tax_rate_chart = create_trend_chart(
                            tax_rate_df,
                            'effective_tax_rate',
                            'Rate (%)',
                            'Effective Tax Rate',
                            is_currency=False
                        )
                        st.altair_chart(tax_rate_chart, width='stretch')
                    else:
                        st.info("Tax rate data unavailable")

                with chart2_col:
                    # Chart 2: Net Taxes
                    net_tax_chart = create_trend_chart(
                        tax_history_df,
                        'net_tax',
                        'Net Tax ($)',
                        'Net Taxes',
                        is_currency=True
                    )
                    st.altair_chart(net_tax_chart, width='stretch')

                with chart3_col:
                    # Chart 3: Assessed Values (Total, Land, Improvement)
                    assessed_value_chart = create_assessed_value_trend_chart(tax_history_df)
                    st.altair_chart(assessed_value_chart, width='stretch')

                # Bottom: Grouped bar chart for tax sources (full width)
                st.markdown("#### Tax Breakdown by Source")
                group_by = st.radio(
                    "Group by:",
                    options=["source", "year"],
                    format_func=lambda x: "By Source (see yearly trends)" if x == "source" else "By Year (see source breakdown)",
                    horizontal=True,
                    label_visibility="collapsed"
                )
                tax_sources_chart = create_tax_sources_chart(tax_history_df, group_by=group_by)
                st.altair_chart(tax_sources_chart, width='stretch')

            elif not tax_history_df.empty and len(tax_history_df) < 2:
                st.info("At least 2 years of data required to show trends")
            else:
                st.info("No historical data available for this parcel")


    else:
        st.error("Parcel data not found.")
