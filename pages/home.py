import streamlit as st

st.title("Madison Tax Parcel Explorer")

st.markdown("""
A tool to help you understand property tax trends and the spatial distribution
of financial metrics across the City of Madison.
""")

st.markdown("### Features")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **Parcel Explorer**

    Search for any property by address and view detailed assessment values,
    tax information, and historical trends.
    """)

with col2:
    st.markdown("""
    **Parcel Map** *(Coming Soon)*

    Visualize property metrics across the city with an interactive map
    and customizable metric overlays.
    """)
