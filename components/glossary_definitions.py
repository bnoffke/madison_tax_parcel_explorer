"""Glossary term definitions for the Madison Tax Parcel Explorer."""

GLOSSARY_TERMS = {
    "metrics": {
        "label": "Calculated Metrics",
        "icon": "📊",
        "terms": {
            "Net Taxes per Sq Ft": {
                "definition": "Net taxes divided by lot size, aggregated to the selected overlay type."
            },
            "Land Value per Sq Ft": {
                "definition": "Land value divided by lot size, aggregated to the selected overlay type.",
                "note": "Can be used to inspect consistency of land value assessments across areas."
            },
            "Land Value Alignment Index": {
                "definition": "Measures alignment between land improvements and land value. Calculated as the ratio of total value share to land value share, relative to citywide assessments.",
                "formula": r"$$\Large \frac{\Sigma \text{Total Value}_{\text{area}} \,/\, \Sigma \text{Total Value}_{\text{city}}}{\Sigma \text{Land Value}_{\text{area}} \,/\, \Sigma \text{Land Value}_{\text{city}}}$$",
                "interpretation": "**< 1** — Underutilized land\n\n**≈ 1** — Appropriately utilized land\n\n**> 1** — Well-utilized land",
                "note": "This interpretation is predicated on accurate land assessment."
            },
            "Taxes per City Street sqft": {
                "definition": "Tax revenue normalized by infrastructure footprint, using city-maintained street area as a proxy. Street area is calculated by multiplying length by recorded width for city-maintained streets only (i.e. excludes highways)."
            },
            "Vehicle Pavement / Dwelling Unit": {
                "definition": "Total vehicle-oriented impervious surface area (parking lots, unpaved parking, driveways, streets, and alleys) divided by the number of dwelling units within the area. Only available at Area Plan and Alder District level.",
                "note": "Dwelling units are used as a proxy for population."
            },
            "Sqft People Space / 100 sqft Vehicle Pavement": {
                "definition": "People-oriented surface area divided by vehicle-oriented surface area, multiplied by 100. A higher value indicates more space dedicated to people relative to vehicles. Only available at Area Plan and Alder District level.",
                "interpretation": "**> 100** — More people space than vehicle pavement\n\n**= 100** — Equal people and vehicle space\n\n**< 100** — More vehicle pavement than people space"
            }
        }
    },
    "overlay_types": {
        "label": "Overlay Types",
        "icon": "🗺️",
        "terms": {
            "Parcel": {
                "definition": "An assessed property representing the total footprint of a parcel site."
            },
            "Area Plan": {
                "definition": "City-designated geographic areas grouping land use plans evaluated every 10 years."
            },
            "Alder District": {
                "definition": "Geographic districts for city council representation."
            }
        }
    },
    "methodology": {
        "label": "Methodology",
        "icon": "📐",
        "terms": {
            "Lot Size": {
                "definition": "Total area of non-exempt parcels (net taxes > 0) within the selected geography. Applied uniformly across all overlay types and used in all per-square-foot calculations."
            },
            "Impervious Surface Classification": {
                "definition": "Impervious surfaces are tagged as either *people* surfaces or *vehicle* surfaces based on their type.",
                "interpretation": "**People surfaces:** Sidewalks, Playgrounds\n\n**Vehicle surfaces:** Parking, Unpaved Parking, Driveways, Streets, Alleys",
                "note": "Surfaces labeled 'Sidewalks' also include shared-use paths and plazas. Surface areas are spatially intersected with each overlay boundary and summed."
            },
            "Dwelling Unit": {
                "definition": "A single residential housing unit (e.g., a house, apartment, or condo). Used here as a proxy for population to normalize surface area metrics per resident.",
                "note": "Available only at Area Plan and Alder District overlay levels."
            }
        }
    }
}
