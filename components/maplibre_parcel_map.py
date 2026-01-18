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
/* Control Panel Styles */
#map-control-panel .mode-btn:hover:not(.active) {
    background: #d0d0d0 !important;
}
#map-control-panel .mode-btn.active {
    background: #4a90d9 !important;
    color: white !important;
}
#map-control-panel #confirm-group-btn:not(:disabled):hover {
    background: #357abd !important;
}
#map-control-panel #confirm-group-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}
#map-control-panel #reset-btn:hover {
    background: #f5f5f5 !important;
    border-color: #999 !important;
}
#map-control-panel #compare-groups-btn:hover {
    background: #218838 !important;
}
#map-control-panel .selection-item {
    padding: 4px 0;
    font-size: 12px;
    color: #555;
    border-bottom: 1px solid #f0f0f0;
}
#map-control-panel .selection-item:last-child {
    border-bottom: none;
}
#map-control-panel .parcel-id {
    color: #999;
    font-size: 10px;
    display: block;
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

    // Group Comparison Mode state
    let selectionMode = 'individual';  // 'individual' | 'group'
    let groupModeState = 'IDLE';  // 'IDLE' | 'SELECTING_G1' | 'SELECTING_G2' | 'COMPLETE'
    let group1Features = [];
    let group2Features = [];
    let confirmedGroup1 = null;  // { features: [...] } or null
    let confirmedGroup2 = null;
    let featureGroupMap = new Map();  // featureId -> 'group1' | 'group2'

    // Color constants for selection outlines
    const SELECTION_COLORS = {
        individual: '#000000',   // Black
        group1: '#00FFFF',       // Cyan
        group2: '#39FF14'        // Lime Green
    };

    // DOM element references (avoid getElementById issues)
    let controlPanel = null;

    // Create HTML structure in JavaScript
    function createElementsAndInit() {
        console.log('Creating HTML elements...');

        // Create map container (v2: append to parentElement, not document.body!)
        mapContainer = document.createElement('div');
        mapContainer.id = 'map-container';
        mapContainer.className = 'map-container';  // Height controlled by CSS!

        // Create unified control panel
        controlPanel = document.createElement('div');
        controlPanel.id = 'map-control-panel';
        controlPanel.style.cssText = 'position: absolute; top: 60px; right: 10px; background: rgba(255,255,255,0.95); padding: 12px 16px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); font-family: system-ui; font-size: 14px; z-index: 1000; min-width: 240px; max-width: 320px;';
        controlPanel.innerHTML = `
            <!-- Mode Toggle Section -->
            <div class="mode-toggle-section" style="margin-bottom: 12px; border-bottom: 1px solid #eee; padding-bottom: 12px;">
                <label style="font-weight: 600; color: #333; display: block; margin-bottom: 6px; font-size: 12px;">Selection Mode</label>
                <div class="segmented-control" style="display: flex; background: #e0e0e0; border-radius: 6px; overflow: hidden;">
                    <button class="mode-btn active" data-mode="individual" style="flex: 1; padding: 8px 12px; border: none; background: #4a90d9; color: white; cursor: pointer; font-size: 13px; transition: all 0.2s;">Individual</button>
                    <button class="mode-btn" data-mode="group" style="flex: 1; padding: 8px 12px; border: none; background: transparent; color: #333; cursor: pointer; font-size: 13px; transition: all 0.2s;">Group</button>
                </div>
            </div>

            <!-- Individual Mode Panel -->
            <div id="individual-mode-panel" style="display: block;">
                <div style="font-weight: 600; color: #333; margin-bottom: 8px;">
                    Selected Parcels (<span id="selection-count">0</span>/2)
                </div>
                <div id="selection-list"></div>
                <div id="individual-empty-state" style="color: #888; font-size: 12px;">Click parcels on the map to select</div>
            </div>

            <!-- Group Mode Panel -->
            <div id="group-mode-panel" style="display: none;">
                <!-- State Indicator -->
                <div id="group-state-indicator" style="font-weight: 600; color: #333; margin-bottom: 8px;">
                    Click parcels to start building Group 1
                </div>

                <!-- Active Group Selection Area -->
                <div id="active-group-section">
                    <div id="active-group-label" style="font-size: 12px; color: #00FFFF; margin-bottom: 4px; font-weight: 600;">Building Group 1:</div>
                    <div id="active-group-count" style="font-size: 18px; font-weight: bold; color: #00FFFF;">0 parcels</div>
                    <div id="active-group-list" style="max-height: 100px; overflow-y: auto; margin: 8px 0; font-size: 12px;"></div>
                </div>

                <!-- Confirmed Groups Summary -->
                <div id="confirmed-groups-summary" style="margin-top: 12px; padding-top: 12px; border-top: 1px solid #eee; display: none;">
                    <div id="confirmed-g1-summary" style="display: none; margin-bottom: 8px;">
                        <span style="color: #00FFFF; font-weight: 600;">● Group 1:</span>
                        <span id="confirmed-g1-count">0 parcels</span>
                    </div>
                    <div id="confirmed-g2-summary" style="display: none; margin-bottom: 8px;">
                        <span style="color: #39FF14; font-weight: 600;">● Group 2:</span>
                        <span id="confirmed-g2-count">0 parcels</span>
                    </div>
                </div>

                <!-- Action Buttons -->
                <div id="group-action-buttons" style="margin-top: 12px; display: flex; gap: 8px;">
                    <button id="confirm-group-btn" disabled style="flex: 1; padding: 10px; border: none; border-radius: 6px; background: #4a90d9; color: white; cursor: pointer; font-weight: 600; opacity: 0.5;">Confirm Group 1</button>
                    <button id="reset-btn" style="padding: 10px 16px; border: 1px solid #ccc; border-radius: 6px; background: white; color: #666; cursor: pointer;">Reset</button>
                </div>

                <!-- Compare Groups Button (shown only in COMPLETE state) -->
                <button id="compare-groups-btn" style="display: none; width: 100%; margin-top: 12px; padding: 12px; border: none; border-radius: 6px; background: #28a745; color: white; cursor: pointer; font-weight: 600; font-size: 14px;">
                    🔍 Compare Groups
                </button>
            </div>
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

        mapContainer.appendChild(controlPanel);
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

    // Helper to get element from control panel (avoid getElementById issues)
    function getElement(selector) {
        return controlPanel ? controlPanel.querySelector(selector) : null;
    }

    // Update individual mode badge
    function updateIndividualBadge() {
        const countSpan = getElement('#selection-count');
        const listDiv = getElement('#selection-list');
        const emptyState = getElement('#individual-empty-state');

        if (!countSpan || !listDiv) return;

        countSpan.textContent = selectedFeatures.length;

        if (selectedFeatures.length === 0) {
            listDiv.innerHTML = '';
            if (emptyState) emptyState.style.display = 'block';
        } else {
            if (emptyState) emptyState.style.display = 'none';
            listDiv.innerHTML = selectedFeatures.map((f, idx) => `
                <div class="selection-item">
                    <strong>${idx + 1}.</strong> ${f.label}
                    <span class="parcel-id">${f.id}</span>
                </div>
            `).join('');
        }
    }

    // Update group mode UI
    function updateGroupModeUI() {
        const stateIndicator = getElement('#group-state-indicator');
        const activeGroupLabel = getElement('#active-group-label');
        const activeGroupCount = getElement('#active-group-count');
        const activeGroupList = getElement('#active-group-list');
        const activeSection = getElement('#active-group-section');
        const confirmedSummary = getElement('#confirmed-groups-summary');
        const confirmBtn = getElement('#confirm-group-btn');
        const compareBtn = getElement('#compare-groups-btn');
        const g1Summary = getElement('#confirmed-g1-summary');
        const g1Count = getElement('#confirmed-g1-count');
        const g2Summary = getElement('#confirmed-g2-summary');
        const g2Count = getElement('#confirmed-g2-count');

        if (!stateIndicator) return;

        // State indicator messages
        const stateMessages = {
            'IDLE': 'Click parcels to start building Group 1',
            'SELECTING_G1': 'Building Group 1',
            'SELECTING_G2': 'Building Group 2',
            'COMPLETE': 'Both groups ready!'
        };
        stateIndicator.textContent = stateMessages[groupModeState] || '';

        // Update active group display based on state
        if (groupModeState === 'SELECTING_G1' || groupModeState === 'IDLE') {
            activeGroupLabel.textContent = 'Building Group 1:';
            activeGroupLabel.style.color = SELECTION_COLORS.group1;
            activeGroupCount.textContent = group1Features.length + ' parcels';
            activeGroupCount.style.color = SELECTION_COLORS.group1;
            activeGroupList.innerHTML = group1Features.map(f =>
                '<div class="selection-item">' + f.label + '</div>'
            ).join('');
            activeSection.style.display = 'block';
        } else if (groupModeState === 'SELECTING_G2') {
            activeGroupLabel.textContent = 'Building Group 2:';
            activeGroupLabel.style.color = SELECTION_COLORS.group2;
            activeGroupCount.textContent = group2Features.length + ' parcels';
            activeGroupCount.style.color = SELECTION_COLORS.group2;
            activeGroupList.innerHTML = group2Features.map(f =>
                '<div class="selection-item">' + f.label + '</div>'
            ).join('');
            activeSection.style.display = 'block';
        } else if (groupModeState === 'COMPLETE') {
            activeSection.style.display = 'none';
        }

        // Update confirmed groups summary
        if (confirmedGroup1 || confirmedGroup2) {
            confirmedSummary.style.display = 'block';

            if (confirmedGroup1) {
                g1Summary.style.display = 'block';
                g1Count.textContent = confirmedGroup1.features.length + ' parcels';
            } else {
                g1Summary.style.display = 'none';
            }

            if (confirmedGroup2) {
                g2Summary.style.display = 'block';
                g2Count.textContent = confirmedGroup2.features.length + ' parcels';
            } else {
                g2Summary.style.display = 'none';
            }
        } else {
            confirmedSummary.style.display = 'none';
        }

        // Update button visibility and state
        if (groupModeState === 'COMPLETE') {
            confirmBtn.style.display = 'none';
            compareBtn.style.display = 'block';
        } else {
            confirmBtn.style.display = 'block';
            compareBtn.style.display = 'none';

            // Update confirm button text and state
            if (groupModeState === 'SELECTING_G1' || groupModeState === 'IDLE') {
                confirmBtn.textContent = 'Confirm Group 1';
                confirmBtn.disabled = group1Features.length === 0;
                confirmBtn.style.opacity = group1Features.length === 0 ? '0.5' : '1';
            } else if (groupModeState === 'SELECTING_G2') {
                confirmBtn.textContent = 'Confirm Group 2';
                confirmBtn.disabled = group2Features.length === 0;
                confirmBtn.style.opacity = group2Features.length === 0 ? '0.5' : '1';
            }
        }
    }

    // Calculate aggregate values for a group
    function calculateGroupAggregate(features) {
        if (!features || features.length === 0) return null;

        const aggregate = {
            count: features.length,
            total_value: 0,
            land_value: 0,
            lot_size: 0,
            net_taxes: 0
        };

        features.forEach(f => {
            aggregate.total_value += f.properties.total_value || 0;
            aggregate.land_value += f.properties.land_value || 0;
            aggregate.lot_size += f.properties.lot_size || 0;
            aggregate.net_taxes += f.properties.net_taxes || 0;
        });

        // Calculate derived metrics
        if (aggregate.lot_size > 0) {
            aggregate.net_taxes_per_sqft = aggregate.net_taxes / aggregate.lot_size;
            aggregate.land_value_per_sqft = aggregate.land_value / aggregate.lot_size;
        } else {
            aggregate.net_taxes_per_sqft = 0;
            aggregate.land_value_per_sqft = 0;
        }

        // Alignment index is average
        const alignmentSum = features.reduce((sum, f) => sum + (f.properties.alignment_index || 0), 0);
        aggregate.alignment_index = alignmentSum / features.length;

        return aggregate;
    }

    // Sync individual mode selection to Python
    function syncToPython() {
        if (selectionMode === 'individual') {
            setStateValue('selected_features', selectedFeatures);
        }
    }

    // Sync group comparison to Python (only called on Compare click)
    function syncGroupsToPython() {
        const payload = {
            comparison_mode: 'group',
            group1: confirmedGroup1 ? {
                features: confirmedGroup1.features,
                aggregate: calculateGroupAggregate(confirmedGroup1.features)
            } : null,
            group2: confirmedGroup2 ? {
                features: confirmedGroup2.features,
                aggregate: calculateGroupAggregate(confirmedGroup2.features)
            } : null
        };
        setStateValue('selected_features', payload);
    }

    // State machine transition
    function transitionGroupState(action) {
        switch (groupModeState) {
            case 'IDLE':
                if (action === 'START_SELECTING') {
                    groupModeState = 'SELECTING_G1';
                }
                break;

            case 'SELECTING_G1':
                if (action === 'CONFIRM' && group1Features.length > 0) {
                    confirmedGroup1 = { features: [...group1Features] };
                    groupModeState = 'SELECTING_G2';
                } else if (action === 'RESET') {
                    resetGroupMode();
                }
                break;

            case 'SELECTING_G2':
                if (action === 'CONFIRM' && group2Features.length > 0) {
                    confirmedGroup2 = { features: [...group2Features] };
                    groupModeState = 'COMPLETE';
                } else if (action === 'RESET') {
                    resetGroupMode();
                }
                break;

            case 'COMPLETE':
                if (action === 'COMPARE') {
                    syncGroupsToPython();  // Trigger Streamlit rerun
                } else if (action === 'RESET') {
                    resetGroupMode();
                }
                break;
        }

        updateGroupModeUI();
    }

    // Clear all group selections and visual states
    function resetGroupMode() {
        // Clear visual states for all group features
        [...group1Features, ...group2Features].forEach(f => {
            const feature = data.geojson.features.find(gf => gf.properties.feature_id === f.id);
            if (feature) {
                map.setFeatureState(
                    { source: 'parcels', id: feature.id },
                    { selected: false, groupMembership: null }
                );
            }
        });

        // Reset state
        group1Features = [];
        group2Features = [];
        confirmedGroup1 = null;
        confirmedGroup2 = null;
        featureGroupMap.clear();
        groupModeState = 'IDLE';

        // Auto-transition to SELECTING_G1
        if (selectionMode === 'group') {
            transitionGroupState('START_SELECTING');
        }
    }

    // Clear all selections (both modes)
    function clearAllSelections() {
        // Clear individual mode selections
        selectedFeatures.forEach(f => {
            const feature = data.geojson.features.find(gf => gf.properties.feature_id === f.id);
            if (feature) {
                map.setFeatureState(
                    { source: 'parcels', id: feature.id },
                    { selected: false, groupMembership: null }
                );
            }
        });
        selectedFeatures = [];

        // Clear group mode selections
        resetGroupMode();
    }

    // Switch between selection modes
    function switchSelectionMode(newMode) {
        if (newMode === selectionMode) return;

        // Clear all selections when switching modes
        clearAllSelections();

        selectionMode = newMode;

        // Update mode button styling
        const modeBtns = controlPanel.querySelectorAll('.mode-btn');
        modeBtns.forEach(btn => {
            if (btn.dataset.mode === newMode) {
                btn.classList.add('active');
                btn.style.background = '#4a90d9';
                btn.style.color = 'white';
            } else {
                btn.classList.remove('active');
                btn.style.background = 'transparent';
                btn.style.color = '#333';
            }
        });

        // Toggle panel visibility
        const individualPanel = getElement('#individual-mode-panel');
        const groupPanel = getElement('#group-mode-panel');

        if (newMode === 'individual') {
            individualPanel.style.display = 'block';
            groupPanel.style.display = 'none';
            updateIndividualBadge();
            // Sync empty state to Python
            syncToPython();
        } else {
            individualPanel.style.display = 'none';
            groupPanel.style.display = 'block';
            // Start group selection
            transitionGroupState('START_SELECTING');
        }
    }

    // Build feature object from properties
    function buildFeatureObject(props, labelValue) {
        return {
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
        };
    }

    // Validate overlay type consistency
    function validateOverlayType(props) {
        if (selectionMode === 'individual' && selectedFeatures.length > 0) {
            if (props.overlay_type !== selectedFeatures[0].overlay_type) {
                console.warn('Cannot compare features from different overlay types');
                return false;
            }
        }
        if (selectionMode === 'group') {
            const existingType = group1Features[0]?.overlay_type || group2Features[0]?.overlay_type;
            if (existingType && props.overlay_type !== existingType) {
                console.warn('Cannot compare features from different overlay types');
                return false;
            }
        }
        return true;
    }

    // Handle individual mode click
    function handleIndividualClick(featureId, props, labelValue) {
        const existingIndex = selectedFeatures.findIndex(f => f.id === props.feature_id);

        if (existingIndex >= 0) {
            // Deselect
            selectedFeatures.splice(existingIndex, 1);
            map.setFeatureState({ source: 'parcels', id: featureId }, { selected: false });
        } else {
            // Select with FIFO replacement
            if (selectedFeatures.length >= MAX_SELECTIONS) {
                const oldestFeature = selectedFeatures.shift();
                const oldFeature = data.geojson.features.find(f => f.properties.feature_id === oldestFeature.id);
                if (oldFeature) {
                    map.setFeatureState({ source: 'parcels', id: oldFeature.id }, { selected: false });
                }
            }

            selectedFeatures.push(buildFeatureObject(props, labelValue));
            map.setFeatureState({ source: 'parcels', id: featureId }, { selected: true });
        }

        updateIndividualBadge();
        syncToPython();
    }

    // Handle group mode click
    function handleGroupClick(featureId, props, labelValue) {
        // Determine which group is active based on state
        const activeGroup = (groupModeState === 'SELECTING_G1' || groupModeState === 'IDLE')
            ? 'group1'
            : 'group2';

        const activeArray = activeGroup === 'group1' ? group1Features : group2Features;
        const existingIndex = activeArray.findIndex(f => f.id === props.feature_id);

        // Check if feature is in the OTHER confirmed group (prevent double-assignment)
        if (activeGroup === 'group2' && confirmedGroup1) {
            const inGroup1 = confirmedGroup1.features.some(f => f.id === props.feature_id);
            if (inGroup1) {
                console.warn('Feature already in Group 1');
                return;
            }
        }

        if (existingIndex >= 0) {
            // Remove from active group
            activeArray.splice(existingIndex, 1);
            featureGroupMap.delete(props.feature_id);
            map.setFeatureState(
                { source: 'parcels', id: featureId },
                { selected: false, groupMembership: null }
            );
        } else {
            // Add to active group (no limit in group mode)
            activeArray.push(buildFeatureObject(props, labelValue));
            featureGroupMap.set(props.feature_id, activeGroup);
            map.setFeatureState(
                { source: 'parcels', id: featureId },
                { selected: true, groupMembership: activeGroup }
            );
        }

        // Update UI (but do NOT sync to Python - that only happens on confirm)
        updateGroupModeUI();
    }

    // Set up control panel event handlers
    function setupControlPanelHandlers() {
        // Mode toggle buttons
        const modeBtns = controlPanel.querySelectorAll('.mode-btn');
        modeBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const newMode = e.target.dataset.mode;
                switchSelectionMode(newMode);
            });
        });

        // Confirm button
        const confirmBtn = getElement('#confirm-group-btn');
        if (confirmBtn) {
            confirmBtn.addEventListener('click', () => {
                transitionGroupState('CONFIRM');
            });
        }

        // Reset button
        const resetBtn = getElement('#reset-btn');
        if (resetBtn) {
            resetBtn.addEventListener('click', () => {
                transitionGroupState('RESET');
            });
        }

        // Compare button
        const compareBtn = getElement('#compare-groups-btn');
        if (compareBtn) {
            compareBtn.addEventListener('click', () => {
                transitionGroupState('COMPARE');
            });
        }
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

        // Line layer for selection outline (supports group colors)
        console.log('Adding selection outline layer...');
        map.addLayer({
            id: 'parcels-line-selected',
            type: 'line',
            source: 'parcels',
            paint: {
                'line-color': [
                    'case',
                    // Group 2 - Lime Green
                    ['==', ['feature-state', 'groupMembership'], 'group2'],
                    SELECTION_COLORS.group2,
                    // Group 1 - Cyan
                    ['==', ['feature-state', 'groupMembership'], 'group1'],
                    SELECTION_COLORS.group1,
                    // Individual mode / fallback - Black
                    ['boolean', ['feature-state', 'selected'], false],
                    SELECTION_COLORS.individual,
                    // Default (not selected)
                    'transparent'
                ],
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

            // Build property classification lines (only for parcels)
            const propertyClassLine = (overlayType === 'parcels' && props.property_class)
                ? `<b>Property Class:</b> ${props.property_class}<br/>`
                : '';
            const propertyUseLine = (overlayType === 'parcels' && props.property_use)
                ? `<b>Property Use:</b> ${props.property_use}<br/>`
                : '';

            // Build city street metric line (only for aggregated overlays)
            const cityStreetLine = (overlayType !== 'parcels' && props.taxes_per_city_street_sqft > 0)
                ? `<b>Taxes/City Street sqft:</b> $${props.taxes_per_city_street_sqft.toFixed(2)}<br/>`
                : '';

            const html = `
                <b>${labelValue}</b><br/>
                ${propertyClassLine}
                ${propertyUseLine}
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

        // Click handler for selection (unified dispatcher)
        console.log('Attaching click handler...');
        map.on('click', 'parcels-fill', (e) => {
            if (!e.features || e.features.length === 0) return;

            const clickedFeature = e.features[0];
            const featureId = clickedFeature.id;
            const props = clickedFeature.properties;
            const labelValue = props[displayField] || 'N/A';

            // Validate overlay type consistency
            if (!validateOverlayType(props)) return;

            // Route to mode-specific handler
            if (selectionMode === 'individual') {
                handleIndividualClick(featureId, props, labelValue);
            } else {
                handleGroupClick(featureId, props, labelValue);
            }
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

        // Set up control panel event handlers
        setupControlPanelHandlers();
        console.log('Control panel handlers attached');

        // Initialize UI state
        updateIndividualBadge();

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
