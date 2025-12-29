"""DuckDB connection management using Streamlit caching."""

import streamlit as st
import duckdb


@st.cache_resource
def get_duckdb_connection():
    """
    Get or create a shared DuckDB connection (app-wide singleton).

    This connection is shared across all user sessions and persists
    for the lifetime of the Streamlit app.

    Returns:
        duckdb.DuckDBPyConnection: Shared DuckDB connection
    """
    conn = duckdb.connect()  # In-memory database

    # Load extensions (once for all sessions)
    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL spatial;
        LOAD spatial;
    """)

    # Create GCS secret (once for all sessions)
    conn.execute(f"""
        CREATE SECRET gcs_secret (
            TYPE gcs,
            KEY_ID '{st.secrets["gcs"]["key_id"]}',
            SECRET '{st.secrets["gcs"]["secret"]}'
        );
    """)

    return conn


@st.cache_data(ttl=3600)  # Cache for 1 hour, shared across sessions
def load_address_data(_conn, silver_bucket: str) -> list[tuple[str, str]]:
    """
    Load parcel addresses for search functionality.

    This data is cached and shared across all user sessions for memory efficiency.
    The underscore prefix on _conn tells Streamlit not to hash the connection object.

    Args:
        _conn: DuckDB connection (not hashed by Streamlit)
        silver_bucket: GCS bucket path for silver layer data

    Returns:
        List of (full_address, parcel_id) tuples
    """
    query = f"""
    SELECT full_address, parcel_id
    FROM read_parquet('{silver_bucket}/fact_parcels.parquet')
    WHERE full_address IS NOT NULL
    ORDER BY full_address
    """
    return _conn.execute(query).fetchall()


def get_connection():
    """
    Get connection and bucket paths.

    Returns:
        tuple: (conn, silver_bucket, gold_bucket)
    """
    conn = get_duckdb_connection()
    silver_bucket = st.secrets["gcs"]["silver_bucket"]
    gold_bucket = st.secrets["gcs"]["gold_bucket"]

    return conn, silver_bucket, gold_bucket
