"""DuckDB connection management for Streamlit session state."""

import streamlit as st
import duckdb


def get_connection():
    """
    Initialize DuckDB connection if not in session state, then return it.

    Returns:
        tuple: (conn, silver_bucket, gold_bucket)
    """
    if "conn" not in st.session_state:
        conn = duckdb.connect()
        conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            INSTALL spatial;
            LOAD spatial;
        """)
        conn.execute(f"""
            CREATE SECRET gcs_secret (
                TYPE gcs,
                KEY_ID '{st.secrets["gcs"]["key_id"]}',
                SECRET '{st.secrets["gcs"]["secret"]}'
            );
        """)
        st.session_state.conn = conn
        st.session_state.SILVER_BUCKET = st.secrets["gcs"]["silver_bucket"]
        st.session_state.GOLD_BUCKET = st.secrets["gcs"]["gold_bucket"]

    return (
        st.session_state.conn,
        st.session_state.SILVER_BUCKET,
        st.session_state.GOLD_BUCKET,
    )
