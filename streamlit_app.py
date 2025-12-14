import streamlit as st

from utils.db import get_connection

st.set_page_config(layout="wide", page_title="Madison Tax Parcel Explorer")

# Initialize shared state (only once per session)
get_connection()

# Define pages
pages = [
    st.Page("pages/home.py", title="Home", icon="🏠", default=True),
    st.Page("pages/parcel_explorer.py", title="Parcel Explorer", icon="🔍"),
    st.Page("pages/parcel_map.py", title="Parcel Map", icon="🗺️"),
]

# Top bar navigation
pg = st.navigation(pages, position="top")
pg.run()
