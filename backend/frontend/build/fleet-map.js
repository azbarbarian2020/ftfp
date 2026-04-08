/**
 * FTFP V2 - Fleet Tracking Map (Static Image Version)
 * Uses static US map image with SVG overlay
 * Fast, simple, no external dependencies
 */
(function() {
  'use strict';

  // State
  let routeGeometries = {};
  let trucks = [];
  let serviceCenters = [];
  let predictions = {};
  let selectedTruck = null;
  let pollInterval = null;
  let svg = null;

  // Map dimensions (matches the static image aspect ratio)
  const MAP_WIDTH = 960;
  const MAP_HEIGHT = 600;

  // Continental US bounds for coordinate conversion
  const BOUNDS = {
    minLat: 24.5,
    maxLat: 49.5,
    minLng: -125.0,
    maxLng: -66.5
  };

  /**
   * Convert lat/lng to SVG coordinates
   */
  function toXY(lat, lng) {
    const x = ((lng - BOUNDS.minLng) / (BOUNDS.maxLng - BOUNDS.minLng)) * MAP_WIDTH;
    const y = ((BOUNDS.maxLat - lat) / (BOUNDS.maxLat - BOUNDS.minLat)) * MAP_HEIGHT;
    return { x: Math.max(10, Math.min(MAP_WIDTH - 10, x)), y: Math.max(10, Math.min(MAP_HEIGHT - 10, y)) };
  }

  /**
   * Initialize the map
   */
  window.initFleetMap = function() {
    const container = document.getElementById('fleet-map');
    if (!container || svg) return;

    console.log('[FleetMap] Initializing static map...');

    // Create static map with SVG overlay
    container.innerHTML = `
      <div id="map-wrapper" style="position:relative;width:100%;height:100%;background:#dbeafe">
        <img src="/us-map.png" alt="US Map" style="width:100%;height:100%;object-fit:contain" onerror="this.style.display='none'"/>
        <svg id="map-svg" viewBox="0 0 ${MAP_WIDTH} ${MAP_HEIGHT}" preserveAspectRatio="xMidYMid meet" 
             style="position:absolute;top:0;left:0;width:100%;height:100%">
          <g id="routes-layer"></g>
          <g id="highlight-layer"></g>
          <g id="services-layer"></g>
          <g id="trucks-layer"></g>
        </svg>
      </div>
    `;

    svg = document.getElementById('map-svg');

    // Load data
    loadRouteGeometries();
    loadServiceCenters();
    loadTrucks();

    // Start polling (every 2.5 seconds)
    pollInterval = setInterval(loadTrucks, 2500);

    // Click outside to deselect
    svg.addEventListener('click', function(e) {
      if (!e.target.closest('.truck-marker, .popup-box')) {
        selectedTruck = null;
        closePopup();
        render();
      }
    });

    console.log('[FleetMap] Initialized');
  };

  /**
   * Load route geometries (one-time)
   */
  async function loadRouteGeometries() {
    try {
      const response = await fetch('/api/route-geometry');
      const data = await response.json();
      if (data.routes) {
        data.routes.forEach(r => { routeGeometries[r.entity_id] = r; });
        console.log(`[FleetMap] Loaded ${data.routes.length} routes`);
        render();
      }
    } catch (e) {
      console.error('[FleetMap] Route load error:', e);
    }
  }

  /**
   * Load service centers (one-time)
   */
  async function loadServiceCenters() {
    try {
      const response = await fetch('/api/service-centers');
      const data = await response.json();
      if (data.service_centers) {
        serviceCenters = data.service_centers;
        console.log(`[FleetMap] Loaded ${serviceCenters.length} service centers`);
        render();
      }
    } catch (e) {
      console.error('[FleetMap] Service centers load error:', e);
    }
  }

  /**
   * Load truck positions (polling)
   */
  async function loadTrucks() {
    try {
      const [locRes, predRes] = await Promise.all([
        fetch('/api/truck-locations/latest?t=' + Date.now()),
        fetch('/api/predictions/latest?t=' + Date.now()).catch(() => ({ json: () => ({}) }))
      ]);

      const locData = await locRes.json();
      const predData = await predRes.json();

      // Build predictions lookup
      predictions = {};
      (predData.predictions || predData || []).forEach(p => {
        const id = p.entity_id || p.ENTITY_ID;
        const cls = p.predicted_class || p.PREDICTED_FAILURE_TYPE || 'NORMAL';
        if (id && cls !== 'NORMAL') predictions[id] = cls;
      });

      trucks = locData.trucks || [];

      // Update status
      const statusEl = document.getElementById('map-status');
      if (statusEl) {
        const online = trucks.length;
        const failures = Object.keys(predictions).length;
        statusEl.textContent = `Epoch: ${locData.current_epoch || 0} | ${online} trucks | ${failures} alerts`;
      }

      render();
    } catch (e) {
      console.error('[FleetMap] Truck load error:', e);
    }
  }

  /**
   * Render everything
   */
  function render() {
    if (!svg) return;

    renderRoutes();
    renderHighlight();
    renderServiceCenters();
    renderTrucks();
  }

  /**
   * Render route lines (faint background)
   */
  function renderRoutes() {
    const layer = document.getElementById('routes-layer');
    if (!layer) return;

    let html = '';
    Object.values(routeGeometries).forEach(route => {
      if (!route.geometry?.coordinates) return;
      
      // Sample every 50th point for performance
      const coords = route.geometry.coordinates;
      const sampled = [];
      for (let i = 0; i < coords.length; i += 50) {
        const p = toXY(coords[i][1], coords[i][0]);
        sampled.push(`${p.x},${p.y}`);
      }
      // Add last point
      const last = coords[coords.length - 1];
      const lastP = toXY(last[1], last[0]);
      sampled.push(`${lastP.x},${lastP.y}`);

      html += `<polyline points="${sampled.join(' ')}" fill="none" stroke="#94a3b8" stroke-width="1.5" opacity="0.3"/>`;
    });

    layer.innerHTML = html;
  }

  /**
   * Render highlighted route for selected truck
   */
  function renderHighlight() {
    const layer = document.getElementById('highlight-layer');
    if (!layer) return;

    if (!selectedTruck) {
      layer.innerHTML = '';
      return;
    }

    const truck = trucks.find(t => t.entity_id === selectedTruck);
    const route = routeGeometries[selectedTruck];
    if (!truck || !route?.geometry?.coordinates) {
      layer.innerHTML = '';
      return;
    }

    const coords = route.geometry.coordinates;
    const currentPos = toXY(truck.latitude, truck.longitude);

    // Find closest point index
    let closestIdx = 0;
    let minDist = Infinity;
    coords.forEach((c, i) => {
      const p = toXY(c[1], c[0]);
      const d = Math.hypot(p.x - currentPos.x, p.y - currentPos.y);
      if (d < minDist) { minDist = d; closestIdx = i; }
    });

    let html = '';

    // Traveled portion (solid blue) - sample for performance
    const traveled = [];
    for (let i = 0; i <= closestIdx; i += 30) {
      const p = toXY(coords[i][1], coords[i][0]);
      traveled.push(`${p.x},${p.y}`);
    }
    traveled.push(`${currentPos.x},${currentPos.y}`);
    html += `<polyline points="${traveled.join(' ')}" fill="none" stroke="#2563eb" stroke-width="4" stroke-linecap="round"/>`;

    // Remaining portion (dashed purple)
    const remaining = [`${currentPos.x},${currentPos.y}`];
    for (let i = closestIdx; i < coords.length; i += 30) {
      const p = toXY(coords[i][1], coords[i][0]);
      remaining.push(`${p.x},${p.y}`);
    }
    const lastCoord = coords[coords.length - 1];
    const destP = toXY(lastCoord[1], lastCoord[0]);
    remaining.push(`${destP.x},${destP.y}`);
    html += `<polyline points="${remaining.join(' ')}" fill="none" stroke="#7c3aed" stroke-width="3" stroke-dasharray="8,4"/>`;

    // Origin marker
    const originP = toXY(coords[0][1], coords[0][0]);
    html += `<circle cx="${originP.x}" cy="${originP.y}" r="8" fill="#2563eb" stroke="#fff" stroke-width="2"/>`;

    // Destination marker
    html += `<circle cx="${destP.x}" cy="${destP.y}" r="8" fill="#7c3aed" stroke="#fff" stroke-width="2"/>`;

    // If failure, show service recommendation
    if (predictions[selectedTruck]) {
      const nearest = findNearestServiceCenter(truck);
      if (nearest) {
        const svcP = toXY(nearest.latitude, nearest.longitude);
        html += `<line x1="${currentPos.x}" y1="${currentPos.y}" x2="${svcP.x}" y2="${svcP.y}" stroke="#dc2626" stroke-width="4" stroke-dasharray="6,3"/>`;
        html += `<circle cx="${svcP.x}" cy="${svcP.y}" r="16" fill="none" stroke="#dc2626" stroke-width="3">
                   <animate attributeName="r" values="16;22;16" dur="1s" repeatCount="indefinite"/>
                 </circle>`;
      }
    }

    layer.innerHTML = html;
  }

  /**
   * Find nearest service center
   */
  function findNearestServiceCenter(truck) {
    let nearest = null;
    let minDist = Infinity;
    serviceCenters.forEach(s => {
      if (!s.latitude) return;
      const d = Math.hypot(truck.latitude - s.latitude, truck.longitude - s.longitude);
      if (d < minDist) { minDist = d; nearest = s; }
    });
    return nearest;
  }

  /**
   * Render service center markers
   */
  function renderServiceCenters() {
    const layer = document.getElementById('services-layer');
    if (!layer) return;

    let html = '';
    serviceCenters.forEach(c => {
      if (!c.latitude) return;
      const p = toXY(c.latitude, c.longitude);
      html += `<g class="svc-marker" data-id="${c.center_id}" style="cursor:pointer">
                 <rect x="${p.x - 10}" y="${p.y - 10}" width="20" height="20" rx="4" fill="#1e40af" stroke="#fff" stroke-width="2"/>
                 <text x="${p.x}" y="${p.y + 4}" text-anchor="middle" fill="#fff" font-size="11" font-weight="bold">S</text>
               </g>`;
    });
    layer.innerHTML = html;
  }

  /**
   * Render truck markers
   */
  function renderTrucks() {
    const layer = document.getElementById('trucks-layer');
    if (!layer) return;

    let html = '';
    trucks.forEach(t => {
      if (!t.latitude || !t.longitude) return;
      
      const p = toXY(t.latitude, t.longitude);
      const num = t.entity_id.replace('TRUCK_0', '').replace('TRUCK_', '');
      const hasFail = predictions[t.entity_id];
      const isSelected = t.entity_id === selectedTruck;
      
      const r = isSelected ? 16 : 12;
      const fill = hasFail ? '#dc2626' : '#16a34a';
      const stroke = isSelected ? '#1d4ed8' : '#fff';
      const strokeWidth = isSelected ? 4 : 2;

      html += `<g class="truck-marker" data-id="${t.entity_id}" style="cursor:pointer">`;
      
      // Selection ring
      if (isSelected) {
        html += `<circle cx="${p.x}" cy="${p.y}" r="${r + 6}" fill="none" stroke="#1d4ed8" stroke-width="3"/>`;
      }
      
      // Failure pulse
      if (hasFail) {
        html += `<circle cx="${p.x}" cy="${p.y}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${strokeWidth}">
                   <animate attributeName="r" values="${r};${r+4};${r}" dur="1s" repeatCount="indefinite"/>
                 </circle>`;
      }
      
      // Main circle
      html += `<circle cx="${p.x}" cy="${p.y}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${strokeWidth}"/>`;
      
      // Number
      html += `<text x="${p.x}" y="${p.y + 4}" text-anchor="middle" fill="#fff" font-size="${isSelected ? 12 : 10}" font-weight="bold">${num}</text>`;
      
      html += `</g>`;
    });

    layer.innerHTML = html;

    // Attach click handlers
    layer.querySelectorAll('.truck-marker').forEach(el => {
      el.onclick = function(e) {
        e.stopPropagation();
        const id = el.dataset.id;
        if (selectedTruck === id) {
          selectedTruck = null;
          closePopup();
        } else {
          selectedTruck = id;
          const truck = trucks.find(t => t.entity_id === id);
          if (truck) showPopup(truck, e);
        }
        render();
      };
    });
  }

  /**
   * Show popup for selected truck
   */
  function showPopup(truck, event) {
    closePopup();

    const hasFail = predictions[truck.entity_id];
    const color = hasFail ? '#dc2626' : '#16a34a';

    let html = `
      <div class="popup-box" id="truck-popup" style="
        position: fixed;
        left: ${Math.min(event.clientX + 15, window.innerWidth - 280)}px;
        top: ${Math.max(event.clientY - 10, 10)}px;
        background: #fff;
        padding: 12px;
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.25);
        z-index: 9999;
        font: 13px system-ui;
        max-width: 260px;
      ">
        <div style="font-weight:700;color:${color};margin-bottom:6px">
          ${hasFail ? '🔴' : '🟢'} ${truck.entity_id}
        </div>
        <div><b>Route:</b> ${truck.route_name || 'Unknown'}</div>
        <div><b>To:</b> ${truck.destination || 'Unknown'}</div>
        <div><b>Progress:</b> ${(truck.percent_complete || 0).toFixed(1)}%</div>
    `;

    if (hasFail) {
      html += `
        <div style="background:#fef2f2;padding:6px;margin-top:8px;border-radius:4px;border:1px solid #fca5a5">
          <div style="color:#dc2626;font-weight:600">⚠️ ${hasFail}</div>
        </div>
      `;

      const nearest = findNearestServiceCenter(truck);
      if (nearest) {
        html += `
          <div style="background:#fef3c7;padding:6px;margin-top:6px;border-radius:4px;border:1px solid #fcd34d;font-size:11px">
            <b>Recommendation:</b> ${nearest.center_name}<br>
            ${nearest.city}, ${nearest.state}
          </div>
        `;
      }
    }

    html += '<div style="color:#888;font-size:10px;margin-top:8px">Click truck again to deselect</div></div>';

    document.body.insertAdjacentHTML('beforeend', html);
  }

  function closePopup() {
    const popup = document.getElementById('truck-popup');
    if (popup) popup.remove();
  }

  // Cleanup
  window.addEventListener('beforeunload', () => { if (pollInterval) clearInterval(pollInterval); });
  
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
    } else {
      if (!pollInterval && svg) { loadTrucks(); pollInterval = setInterval(loadTrucks, 2500); }
    }
  });

})();
