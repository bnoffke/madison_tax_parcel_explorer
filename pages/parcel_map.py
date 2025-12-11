import streamlit as st

st.title("Parcel Map")

metric = st.selectbox("Select Metric", [
    "Assessed Value",
    "Net Taxes",
    "Effective Tax Rate",
    "Land Value Share"
])

st.info(f"Map visualization for **{metric}** coming soon...")

st.markdown("""
This page will display an interactive map of Madison parcels with color-coded
overlays based on the selected metric.
""")
