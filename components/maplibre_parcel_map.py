"""MapLibre-based parcel map component with no-rerender selection support."""

import streamlit as st


# HTML template (just libraries)
COMPONENT_HTML = """
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet" />
"""

# CSS for component styling (height controlled here, not via parameter!)
COMPONENT_CSS = """
.map-container {
    width: 100%;
    height: 700px;
    position: relative;
}
.maplibregl-popup-content {
    background: steelblue;
    color: white;
    font-size: 12px;
    padding: 10px;
    max-width: 300px;
}
.maplibregl-popup-content b {
    font-weight: 600;
}
.maplibregl-popup-close-button {
    color: white;
}
"""

# JavaScript component logic
COMPONENT_JS = """
export default function(component) {
    const { parentElement, data, setStateValue } = component;

    console.log('Component function called');
    console.log('Data received, features:', data.geojson ? data.geojson.features.length : 'none');

    // Extract overlay config
    const overlayConfig = data.overlay || {
        display_name_field: 'address',
        overlay_type: 'parcels'
    };
    const displayField = overlayConfig.display_name_field;
    const overlayType = overlayConfig.overlay_type;

    // Selection state
    let selectedFeatures = [];
    const MAX_SELECTIONS = 2;
    let map = null;
    let mapContainer = null;  // Outer scope so initMap() can access it
    let loadingOverlay = null;  // Outer scope so we can remove it after map loads

    // Create HTML structure in JavaScript
    function createElementsAndInit() {
        console.log('Creating HTML elements...');

        // Create map container (v2: append to parentElement, not document.body!)
        mapContainer = document.createElement('div');
        mapContainer.id = 'map-container';
        mapContainer.className = 'map-container';  // Height controlled by CSS!

        // Create selection badge
        const badge = document.createElement('div');
        badge.id = 'selection-badge';
        badge.style.cssText = 'position: absolute; top: 10px; right: 10px; background: rgba(255,255,255,0.95); padding: 12px 16px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); font-family: system-ui; font-size: 14px; z-index: 1000; min-width: 200px; display: none;';
        badge.innerHTML = `
            <div style="font-weight: 600; color: #333; margin-bottom: 8px;">
                Selected Parcels (<span id="selection-count">0</span>/2)
            </div>
            <div id="selection-list"></div>
        `;

        // Create loading overlay
        loadingOverlay = document.createElement('div');
        loadingOverlay.id = 'loading-overlay';
        loadingOverlay.style.cssText = 'position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: rgba(255,255,255,0.95); display: flex; align-items: center; justify-content: center; z-index: 9999;';
        loadingOverlay.innerHTML = `
            <div style="text-align: center;">
                <div style="font-size: 18px; font-weight: bold; color: #333; margin-bottom: 10px;">Loading map...</div>
                <div style="font-size: 14px; color: #666;">Rendering parcels</div>
            </div>
        `;

        mapContainer.appendChild(badge);
        mapContainer.appendChild(loadingOverlay);

        parentElement.appendChild(mapContainer);  // v2: Use parentElement!
        console.log('HTML elements created and appended');

        // Now initialize the map
        initMap();
    }

    // Wait for MapLibre to load and initialize map
    function initMap() {
        if (typeof maplibregl === 'undefined') {
            console.log('Waiting for MapLibre GL to load...');
            setTimeout(initMap, 50);
            return;
        }

        console.log('MapLibre GL loaded');
        console.log('GeoJSON features:', data.geojson.features.length);

        // Initialize map (v2: pass the DOM element, not an ID string!)
        console.log('Creating MapLibre map instance...');
        map = new maplibregl.Map({
            container: mapContainer,
            style: {
                version: 8,
                sources: {
                    'carto-light': {
                        type: 'raster',
                        tiles: [
                            'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
                            'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
                            'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'
                        ],
                        tileSize: 256,
                        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
                    }
                },
                layers: [
                    {
                        id: 'carto-light-layer',
                        type: 'raster',
                        source: 'carto-light',
                        minzoom: 0,
                        maxzoom: 22
                    }
                ],
                glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf'
            },
            center: [data.center.lon, data.center.lat],
            zoom: data.zoom,
            pitch: 0,
            bearing: 0
        });

        console.log('Map object created, waiting for load event...');

    // Add navigation controls
    map.addControl(new maplibregl.NavigationControl(), 'top-right');

    // Popup for tooltips
    const popup = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false,
        maxWidth: '300px'
    });

    // Update selection badge
    function updateSelectionBadge() {
        const badge = document.getElementById('selection-badge');
        const countSpan = document.getElementById('selection-count');
        const listDiv = document.getElementById('selection-list');

        if (!badge || !countSpan || !listDiv) return;

        if (selectedFeatures.length === 0) {
            badge.style.display = 'none';
            return;
        }

        badge.style.display = 'block';
        countSpan.textContent = selectedFeatures.length;

        listDiv.innerHTML = selectedFeatures.map((f, idx) => `
            <div class="selection-item">
                <strong>${idx + 1}.</strong> ${f.label}
                <span class="parcel-id">${f.id}</span>
            </div>
        `).join('');
    }

    // Sync selection to Python (v2: use setStateValue for persistent state)
    function syncToPython() {
        setStateValue('selected_features', selectedFeatures);
    }

    // Map load event
    map.on('load', () => {
        console.log('Map load event fired!');

        // Add parcel source
        console.log('Adding parcel source...');
        map.addSource('parcels', {
            type: 'geojson',
            data: data.geojson
        });
        console.log('Parcel source added');

        // Fill layer with colors from properties (now CSS strings)
        console.log('Adding fill layer...');
        map.addLayer({
            id: 'parcels-fill',
            type: 'fill',
            source: 'parcels',
            paint: {
                'fill-color': ['get', 'fillColor'],
                'fill-opacity': [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false],
                    0.9,
                    0.7
                ]
            }
        });
        console.log('Fill layer added');

        // Line layer for selection outline
        console.log('Adding selection outline layer...');
        map.addLayer({
            id: 'parcels-line-selected',
            type: 'line',
            source: 'parcels',
            paint: {
                'line-color': '#000000',
                'line-width': [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false],
                    3,
                    0
                ]
            }
        });
        console.log('Selection outline layer added');

        // Subtle default borders
        console.log('Adding default border layer...');
        map.addLayer({
            id: 'parcels-line-default',
            type: 'line',
            source: 'parcels',
            paint: {
                'line-color': '#ffffff',
                'line-width': 0.5,
                'line-opacity': 0.3
            }
        });
        console.log('Default border layer added');

        // Hover cursor
        console.log('Attaching hover handlers...');
        map.on('mouseenter', 'parcels-fill', () => {
            map.getCanvas().style.cursor = 'pointer';
        });

        map.on('mouseleave', 'parcels-fill', () => {
            map.getCanvas().style.cursor = '';
        });
        console.log('Hover handlers attached');

        // Hover tooltip
        console.log('Attaching tooltip handlers...');
        map.on('mousemove', 'parcels-fill', (e) => {
            if (!e.features || e.features.length === 0) return;

            const props = e.features[0].properties;
            const labelValue = props[displayField] || 'N/A';

            // Build city street metric line (only for aggregated overlays)
            const cityStreetLine = (overlayType !== 'parcels' && props.taxes_per_city_street_sqft > 0)
                ? `<b>Taxes/City Street sqft:</b> $${props.taxes_per_city_street_sqft.toFixed(2)}<br/>`
                : '';

            const html = `
                <b>${labelValue}</b><br/>
                <b>Assessed Value:</b> ${props.display_total_value}<br/>
                <b>Land Value:</b> ${props.display_land_value}<br/>
                <b>Lot Size:</b> ${props.display_lot_size}<br/>
                <b>Net Taxes:</b> ${props.display_net_taxes}<br/>
                <hr style="margin: 5px 0; border: none; border-top: 1px solid rgba(255,255,255,0.3);"/>
                <b>Net Taxes/sqft:</b> $${props.net_taxes_per_sqft.toFixed(2)}<br/>
                ${cityStreetLine}
                <b>Land Value/sqft:</b> $${props.land_value_per_sqft.toFixed(2)}<br/>
                <b>Alignment Index:</b> ${props.alignment_index.toFixed(2)}
            `;

            popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
        });

        map.on('mouseleave', 'parcels-fill', () => {
            popup.remove();
        });
        console.log('Tooltip handlers attached');

        // Click handler for selection
        console.log('Attaching click handler...');
        map.on('click', 'parcels-fill', (e) => {
            if (!e.features || e.features.length === 0) return;

            const clickedFeature = e.features[0];
            const featureId = clickedFeature.id;
            const props = clickedFeature.properties;
            const labelValue = props[displayField] || 'N/A';

            // Check if already selected
            const existingIndex = selectedFeatures.findIndex(
                f => f.id === props.feature_id
            );

            if (existingIndex >= 0) {
                // Deselect
                selectedFeatures.splice(existingIndex, 1);
                map.setFeatureState(
                    { source: 'parcels', id: featureId },
                    { selected: false }
                );
            } else {
                // Validate overlay type before adding new selection
                if (selectedFeatures.length > 0) {
                    const firstOverlayType = selectedFeatures[0].overlay_type;
                    if (props.overlay_type !== firstOverlayType) {
                        console.warn('Cannot compare features from different overlay types');
                        return;
                    }
                }

                // Select
                if (selectedFeatures.length >= MAX_SELECTIONS) {
                    // Remove oldest selection (FIFO)
                    const oldestFeature = selectedFeatures.shift();

                    // Find the numeric ID for the old feature
                    const oldFeature = data.geojson.features.find(
                        f => f.properties.feature_id === oldestFeature.id
                    );
                    if (oldFeature) {
                        map.setFeatureState(
                            { source: 'parcels', id: oldFeature.id },
                            { selected: false }
                        );
                    }
                }

                // Add new selection
                selectedFeatures.push({
                    id: props.feature_id,
                    label: labelValue,
                    overlay_type: props.overlay_type,
                    properties: {
                        total_value: props.total_value,
                        land_value: props.land_value,
                        lot_size: props.lot_size,
                        net_taxes: props.net_taxes,
                        net_taxes_per_sqft: props.net_taxes_per_sqft,
                        taxes_per_city_street_sqft: props.taxes_per_city_street_sqft || 0,
                        land_value_per_sqft: props.land_value_per_sqft,
                        alignment_index: props.alignment_index
                    }
                });

                map.setFeatureState(
                    { source: 'parcels', id: featureId },
                    { selected: true }
                );
            }

            // Update badge and sync to Python
            updateSelectionBadge();
            syncToPython();
        });
        console.log('Click handler attached');

        console.log('All layers and handlers initialized');

        // Remove loading overlay from DOM (using direct reference, not getElementById)
        console.log('Removing loading overlay...');
        if (loadingOverlay) {
            loadingOverlay.remove();
            console.log('Loading overlay removed - map ready!');
        } else {
            console.warn('Loading overlay reference is null');
        }

        // Initial sync
        syncToPython();
        console.log('Map initialization complete');
    });

    // Add error handler for map
    map.on('error', (e) => {
        console.error('MapLibre error:', e);
    });
    }

    // Start initialization
    console.log('Starting component initialization...');
    createElementsAndInit();

    // Return cleanup function
    return () => {
        if (map) {
            map.remove();
        }
    };
}
"""

# Register the component (v2: name, html, css, js - NO height parameter!)
polygon_map = st.components.v2.component(
    "maplibre_parcel_map",
    html=COMPONENT_HTML,
    css=COMPONENT_CSS,
    js=COMPONENT_JS
)


def render_maplibre_map(geojson_data: dict, center: list, zoom: int, overlay_config: dict):
    """
    Render MapLibre map component with parcel selection.

    Args:
        geojson_data: GeoJSON FeatureCollection
        center: [lat, lon] for map center
        zoom: Initial zoom level
        overlay_config: Dict with display_name_field and overlay_type

    Returns:
        dict: Component value with selected_features
    """
    return polygon_map(
        data={
            "geojson": geojson_data,
            "center": {"lat": center[0], "lon": center[1]},
            "zoom": zoom,
            "overlay": overlay_config
        },
        on_selected_features_change=lambda: None  # Required for v2 state capture
    )
