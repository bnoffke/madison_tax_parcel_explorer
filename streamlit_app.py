import streamlit as st
import duckdb

st.set_page_config(layout="wide", page_title="Madison Tax Parcel Explorer")

# Initialize shared state (only once per session)
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

# Define pages
pages = [
    st.Page("pages/home.py", title="Home", icon="🏠", default=True),
    st.Page("pages/parcel_explorer.py", title="Parcel Explorer", icon="🔍"),
    st.Page("pages/parcel_map.py", title="Parcel Map", icon="🗺️"),
]

# Top bar navigation
pg = st.navigation(pages, position="top")
pg.run()
