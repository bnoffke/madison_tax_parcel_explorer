"""DuckDB connection management using Streamlit caching."""

import streamlit as st
from stmsn_query import connect


@st.cache_resource(ttl=3600)  # Reconnect hourly; the catalog is replaced daily
def get_duckdb_connection():
    """
    Get or create a shared DuckDB connection to the stmsn lakehouse.

    This connection is shared across all user sessions. The DuckLake catalog is
    attached READ_ONLY as `stmsn` and `USE stmsn` is applied, so queries can
    reference tables as `<schema>.<table>`.

    Returns:
        duckdb.DuckDBPyConnection: Shared DuckDB connection
    """
    return connect(
        key_id=st.secrets["gcs"]["key_id"],
        secret=st.secrets["gcs"]["secret"],
    )


@st.cache_data(ttl=3600)  # Cache for 1 hour, shared across sessions
def load_address_data(_conn) -> list[tuple[str, str]]:
    """
    Load parcel addresses for search functionality.

    This data is cached and shared across all user sessions for memory efficiency.
    The underscore prefix on _conn tells Streamlit not to hash the connection object.

    Args:
        _conn: DuckDB connection (not hashed by Streamlit)

    Returns:
        List of (full_address, parcel_id) tuples
    """
    query = """
    SELECT full_address, parcel_id
    FROM silver.fact_parcels
    WHERE full_address IS NOT NULL
    AND parcel_year = (SELECT MAX(parcel_year) FROM silver.fact_parcels)
    ORDER BY full_address
    """
    return _conn.execute(query).fetchall()


def get_connection():
    """
    Get the shared lakehouse connection.

    Returns:
        duckdb.DuckDBPyConnection
    """
    return get_duckdb_connection()
