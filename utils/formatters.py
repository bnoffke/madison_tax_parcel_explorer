"""Shared formatting helper functions for display values."""

import pandas as pd


def format_currency(value) -> str:
    """Format a numeric value as currency."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        return f"${value:,.0f}"
    except (ValueError, TypeError):
        return "N/A"


def format_percentage(value, decimals=1) -> str:
    """Format a numeric value as percentage."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        return f"{value:.{decimals}f}%"
    except (ValueError, TypeError):
        return "N/A"


def format_number(value, decimals=0) -> str:
    """Format a numeric value with commas."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        if decimals == 0:
            return f"{value:,.0f}"
        else:
            return f"{value:,.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"


def format_tax_change(current_taxes, shift_taxes) -> str:
    """Format tax change with arrow and dollar/percentage."""
    if current_taxes is None or shift_taxes is None or pd.isna(current_taxes) or pd.isna(shift_taxes):
        return "N/A"

    try:
        difference = shift_taxes - current_taxes
        if current_taxes == 0:
            return "N/A (no current taxes)"

        pct_change = (difference / current_taxes) * 100

        if difference > 0:
            arrow = "↑"
            sign = "+"
        elif difference < 0:
            arrow = "↓"
            sign = ""
        else:
            return "No change"

        return f"{arrow} {sign}${abs(difference):,.0f} ({sign}{pct_change:.1f}%)"
    except (ValueError, TypeError, ZeroDivisionError):
        return "N/A"


def format_address(parcel_data: dict) -> str:
    """Format a full address from parcel data."""
    if not parcel_data:
        return "N/A"

    try:
        parts = []

        # House number
        if house_nbr := parcel_data.get('house_nbr'):
            parts.append(str(house_nbr))

        # Street direction
        if street_dir := parcel_data.get('street_dir'):
            if street_dir.strip():
                parts.append(street_dir)

        # Street name
        if street_name := parcel_data.get('street_name'):
            parts.append(street_name)

        # Street type
        if street_type := parcel_data.get('street_type'):
            if street_type.strip():
                parts.append(street_type)

        # Unit
        if unit := parcel_data.get('unit'):
            if str(unit).strip():
                parts.append(f"Unit {unit}")

        return " ".join(parts) if parts else "N/A"
    except (ValueError, TypeError):
        return "N/A"
