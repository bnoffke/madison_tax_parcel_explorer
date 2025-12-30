"""Reusable glossary dialog component for displaying term definitions."""

import streamlit as st


def render_glossary_button(button_label="📚 Glossary", help_text="View definitions and methodology", glossary_terms=None):
    """
    Render a button that opens a glossary dialog when clicked.

    Args:
        button_label: Text for the button (default: "📚 Glossary")
        help_text: Tooltip text for the button
        glossary_terms: Optional custom glossary dict (defaults to GLOSSARY_TERMS from glossary_definitions)
    """
    if glossary_terms is None:
        from components.glossary_definitions import GLOSSARY_TERMS
        glossary_terms = GLOSSARY_TERMS

    # Button to trigger dialog
    if st.button(button_label, help=help_text, use_container_width=True):
        show_glossary_dialog(glossary_terms)


@st.dialog("Glossary", width="large")
def show_glossary_dialog(glossary_terms):
    """
    Display glossary content in a modal dialog.

    Args:
        glossary_terms: Dictionary of glossary terms organized by category
    """
    st.markdown("### Definitions and Methodology")

    # Render each category
    for category_key, category_data in glossary_terms.items():
        icon = category_data.get('icon', '•')
        label = category_data.get('label', category_key.title())

        with st.expander(f"{icon} {label}", expanded=True):
            terms = category_data.get('terms', {})
            for term_name, term_data in terms.items():
                st.markdown(f"**{term_name}**")

                if 'definition' in term_data:
                    st.markdown(term_data['definition'])

                if 'formula' in term_data:
                    st.markdown(term_data['formula'])
                    st.markdown("")

                if 'interpretation' in term_data:
                    st.markdown("**Interpretation:**")
                    st.markdown(term_data['interpretation'])

                if 'note' in term_data:
                    st.caption(f"_Note: {term_data['note']}_")

                # Add spacing between terms
                st.markdown("")
