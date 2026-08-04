"""Query-param (URL) state helpers for shareable app views.

Encodes the Parcel Map sidebar selections into the browser query string so a view
can be bookmarked, refreshed, or shared as a link.

SHARE_BASE_URL is the deployed URL used for the "Share this view" link — it is the
one thing to change if the deployment URL changes. It is intentionally hardcoded
rather than derived from st.context.url, which would emit localhost during local dev.
"""

from urllib.parse import urlencode

import streamlit as st

PARAM_OVERLAY = "ov"
PARAM_METRIC = "metric"
PARAM_AREA_PLANS = "ap"
PARAM_ALDER_DISTRICTS = "ad"
PARAM_PROPERTY_CLASS = "pc"
PARAM_PROPERTY_USE = "pu"

# Params this module owns; anything else in the query string is left alone.
MAP_PARAM_KEYS = (
    PARAM_OVERLAY,
    PARAM_METRIC,
    PARAM_AREA_PLANS,
    PARAM_ALDER_DISTRICTS,
    PARAM_PROPERTY_CLASS,
    PARAM_PROPERTY_USE,
)

SHARE_BASE_URL = "https://madison-tax-explorer.streamlit.app/parcel_map"


def _dedupe(values: list[str]) -> list[str]:
    """Drop blanks and duplicates, preserving first-seen order."""
    seen = set()
    result = []
    for value in values:
        cleaned = (value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _get_single(key: str) -> str | None:
    """Read a single-valued param, taking the first value if repeated."""
    values = _dedupe(st.query_params.get_all(key))
    return values[0] if values else None


def decode_map_state(valid_overlays: list[str], valid_metric_columns: set[str]) -> dict:
    """Read map state out of st.query_params.

    Validates only what can be checked without touching the data (overlay key and
    metric column). Unknown or blank values become None / []; never raises. The
    caller is responsible for validating filter values against the cascading
    option lists.

    Returns:
        Dict with keys: overlay_type, metric_column, area_plans, alder_districts,
        property_class, property_use
    """
    overlay_type = _get_single(PARAM_OVERLAY)
    if overlay_type not in valid_overlays:
        overlay_type = None

    metric_column = _get_single(PARAM_METRIC)
    if metric_column not in valid_metric_columns:
        metric_column = None

    return {
        "overlay_type": overlay_type,
        "metric_column": metric_column,
        "area_plans": _dedupe(st.query_params.get_all(PARAM_AREA_PLANS)),
        "alder_districts": _dedupe(st.query_params.get_all(PARAM_ALDER_DISTRICTS)),
        "property_class": _get_single(PARAM_PROPERTY_CLASS),
        "property_use": _get_single(PARAM_PROPERTY_USE),
    }


def encode_map_state(
    *,
    overlay_type: str,
    metric_column: str,
    area_plans: list[str],
    alder_districts: list[str],
    property_class: str | None,
    property_use: str | None,
) -> dict[str, list[str]]:
    """Build the canonical param dict for the current selections.

    Overlay and metric are always emitted so the URL is self-describing. Parcel
    filters are omitted when empty, and entirely when the overlay is not parcels.
    """
    params: dict[str, list[str]] = {
        PARAM_OVERLAY: [overlay_type],
        PARAM_METRIC: [metric_column],
    }

    if overlay_type == "parcels":
        if area_plans:
            params[PARAM_AREA_PLANS] = list(area_plans)
        if alder_districts:
            params[PARAM_ALDER_DISTRICTS] = list(alder_districts)
        if property_class:
            params[PARAM_PROPERTY_CLASS] = [property_class]
        if property_use:
            params[PARAM_PROPERTY_USE] = [property_use]

    return params


def sync_query_params(desired: dict[str, list[str]]) -> None:
    """Make the browser URL match `desired`, preserving any params we don't own.

    Writing query params only updates the URL in place (no rerun, no history
    entry), and the comparison below means a no-change run writes nothing at all.
    """
    current = {key: st.query_params.get_all(key) for key in st.query_params.keys()}

    merged = {key: value for key, value in current.items() if key not in MAP_PARAM_KEYS}
    merged.update({key: [str(v) for v in value] for key, value in desired.items() if value})

    if merged != current:
        st.query_params.from_dict(merged)


def build_share_url(desired: dict[str, list[str]], base_url: str = SHARE_BASE_URL) -> str:
    """Build the full shareable URL for the given params."""
    if not desired:
        return base_url
    return f"{base_url}?{urlencode(desired, doseq=True)}"


def render_share_control(
    share_url: str,
    *,
    label: str = "Share this view",
    icon: str = ":material/link:",
) -> None:
    """Render a popover exposing the shareable URL with a native copy button."""
    with st.popover(
        label,
        icon=icon,
        width="stretch",
        help="Copy a link that reproduces the current filters",
    ):
        st.caption("Copy this link to share the current view:")
        st.code(share_url, language=None, wrap_lines=True)
