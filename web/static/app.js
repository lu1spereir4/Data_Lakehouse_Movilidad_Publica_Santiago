const runBtn = document.getElementById("runQuery");
const downloadBtn = document.getElementById("downloadCsv");
const queryTypeEl = document.getElementById("queryType");
const cutFromEl = document.getElementById("cutFrom");
const cutToEl = document.getElementById("cutTo");
const limitEl = document.getElementById("limit");
const hourFromEl = document.getElementById("hourFrom");
const hourToEl = document.getElementById("hourTo");
const healthBadge = document.getElementById("healthBadge");
const rowCount = document.getElementById("rowCount");
const queryMeta = document.getElementById("queryMeta");
const kpiStrip = document.getElementById("kpiStrip");
const insightsList = document.getElementById("insightsList");
const mapPointCount = document.getElementById("mapPointCount");
const geoCoverage = document.getElementById("geoCoverage");
const tableHead = document.querySelector("#resultTable thead");
const tableBody = document.querySelector("#resultTable tbody");
const streetViewTitle = document.getElementById("streetViewTitle");
const streetViewLink = document.getElementById("streetViewLink");
const streetViewFrame = document.getElementById("streetViewFrame");

let lastRows = [];
let map;
let mapLayer;
let currentMapPoints = [];
let markerByPointKey = new Map();
let selectedRowKey = null;

const COLUMN_META = {
  viajes_observados: { label: "Viajes observados", decimals: 0 },
  viajes_estimados: { label: "Viajes estimados (expandidos)", decimals: 0 },
  etapas_observadas: { label: "Etapas observadas", decimals: 0 },
  etapas_estimadas: { label: "Etapas estimadas (expandidas)", decimals: 0 },
  subidas_promedio_total: { label: "Subidas promedio totales", decimals: 0 },
  mode_code: { label: "Modo" },
  tipo_dia: { label: "Tipo de dia" },
  stop_code: { label: "Paradero" },
  comuna: { label: "Comuna" },
};

function selectedValues(containerId) {
  return Array.from(document.querySelectorAll(`#${containerId} .chip.active`)).map((el) => el.dataset.value);
}

function normalizeKey(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/\s+/g, "")
    .trim();
}

function stopPrefix(value) {
  const norm = normalizeKey(value);
  const parts = norm.split("-");
  if (parts.length <= 1) return norm;
  return parts.slice(0, -1).join("-");
}

function rowKeyFromData(stopCode, modeCode) {
  return `${normalizeKey(stopCode)}|${normalizeKey(modeCode || "")}`;
}

function rowKeyFromRow(row) {
  return rowKeyFromData(row?.stop_code, row?.mode_code);
}

function pointKey(point) {
  return rowKeyFromData(point?.stop_code, point?.mode_code);
}

function applySelectedRowStyle() {
  document.querySelectorAll("#resultTable tbody tr").forEach((tr) => {
    if (tr.dataset.rowKey && tr.dataset.rowKey === selectedRowKey) {
      tr.classList.add("selected-row");
    } else {
      tr.classList.remove("selected-row");
    }
  });
}

function scrollToSelectedRow() {
  if (!selectedRowKey) return;
  const tr = document.querySelector(`#resultTable tbody tr[data-row-key="${selectedRowKey}"]`);
  if (!tr) return;
  tr.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function focusMapByRowKey(targetKey) {
  if (!targetKey || !map) return;
  const point = currentMapPoints.find((p) => pointKey(p) === targetKey);
  if (!point) return;

  const marker = markerByPointKey.get(targetKey);
  const lat = Number(point.lat);
  const lon = Number(point.lon);
  if (Number.isFinite(lat) && Number.isFinite(lon)) {
    map.setView([lat, lon], Math.max(map.getZoom(), 14), { animate: true });
    setStreetView(lat, lon, point.stop_code, false);
  }
  if (marker) marker.openPopup();
  scrollToSelectedRow();
}

function clampHour(value, fallbackValue) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallbackValue;
  return Math.max(0, Math.min(23, Math.trunc(n)));
}

function setStreetView(lat, lon, stopCode = "Paradero seleccionado", isApprox = false) {
  const panoUrl = `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${lat},${lon}`;
  const embedUrl = `https://maps.google.com/maps?q=&layer=c&cbll=${lat},${lon}&cbp=11,0,0,0,0&output=svembed`;
  if (streetViewFrame) streetViewFrame.src = embedUrl;
  if (streetViewLink) streetViewLink.href = panoUrl;
  if (streetViewTitle) {
    streetViewTitle.textContent = isApprox
      ? `Street View: ${stopCode} (sin precision)`
      : `Street View: ${stopCode}`;
  }
}

function bindChips(containerId) {
  document.querySelectorAll(`#${containerId} .chip`).forEach((chip) => {
    chip.addEventListener("click", () => chip.classList.toggle("active"));
  });
}

function getColLabel(colName) {
  return COLUMN_META[colName]?.label || colName;
}

function formatValue(value, colName = "") {
  if (typeof value === "number") {
    const decimals = COLUMN_META[colName]?.decimals;
    if (typeof decimals === "number") {
      return new Intl.NumberFormat("es-CL", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      }).format(value);
    }
    if (Number.isInteger(value)) {
      return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 0 }).format(value);
    }
    return new Intl.NumberFormat("es-CL", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
  }
  return value ?? "";
}

function isNumericColumn(rows, colName) {
  return rows.length > 0 && rows.every((r) => typeof r[colName] === "number");
}

function renderTable(rows) {
  tableHead.innerHTML = "";
  tableBody.innerHTML = "";

  if (!rows.length) {
    rowCount.textContent = "0 filas";
    return;
  }

  const columns = Object.keys(rows[0]);
  const numericCols = new Set(columns.filter((c) => isNumericColumn(rows, c)));
  const trHead = document.createElement("tr");
  columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = getColLabel(c);
    if (numericCols.has(c)) th.classList.add("numeric");
    trHead.appendChild(th);
  });
  tableHead.appendChild(trHead);

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const rk = rowKeyFromRow(row);
    tr.dataset.rowKey = rk;

    if (row.stop_code) {
      tr.addEventListener("click", () => {
        selectedRowKey = rk;
        applySelectedRowStyle();
        scrollToSelectedRow();
        focusMapByRowKey(rk);
      });
    }

    columns.forEach((c) => {
      const td = document.createElement("td");
      td.textContent = formatValue(row[c], c);
      if (numericCols.has(c)) td.classList.add("numeric");
      tr.appendChild(td);
    });
    tableBody.appendChild(tr);
  });

  rowCount.textContent = `${rows.length} fila(s)`;
  applySelectedRowStyle();
}

function setInsights(lines) {
  insightsList.innerHTML = "";
  const items = lines.length ? lines : ["No hay insights para este conjunto de datos."];
  items.forEach((line) => {
    const li = document.createElement("li");
    li.textContent = line;
    insightsList.appendChild(li);
  });
}

function initMap() {
  if (map) return;
  map = L.map("mapCanvas", {
    zoomControl: true,
  }).setView([-33.4489, -70.6693], 11);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18,
  }).addTo(map);

  mapLayer = L.layerGroup().addTo(map);
  setStreetView(-33.4489, -70.6693, "Santiago Centro", true);
}

function markerRadius(value) {
  const n = Number(value || 0);
  if (n <= 0) return 4;
  const r = Math.sqrt(n) / 3;
  return Math.max(4, Math.min(18, r));
}

function getMarkerStyle(value, maxValue) {
  const ratio = maxValue > 0 ? value / maxValue : 0;
  if (ratio >= 0.75) {
    return { color: "#991b1b", fillColor: "#dc2626" };
  }
  if (ratio >= 0.5) {
    return { color: "#b45309", fillColor: "#f97316" };
  }
  if (ratio >= 0.25) {
    return { color: "#a16207", fillColor: "#eab308" };
  }
  return { color: "#0b5d57", fillColor: "#0f766e" };
}

function renderMap(points, pointCountLabel = null) {
  initMap();
  mapLayer.clearLayers();
  markerByPointKey = new Map();
  currentMapPoints = points;

  if (!points.length) {
    mapPointCount.textContent = "0 puntos";
    setStreetView(-33.4489, -70.6693, "Sin punto seleccionado", true);
    return;
  }

  const maxValue = Math.max(...points.map((p) => Number(p.etapas_estimadas || 0)), 0);
  const bounds = [];
  points.forEach((p) => {
    const lat = Number(p.lat);
    const lon = Number(p.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

    const val = Number(p.etapas_estimadas || 0);
    const palette = getMarkerStyle(val, maxValue);

    const circle = L.circleMarker([lat, lon], {
      radius: markerRadius(val),
      color: palette.color,
      weight: 1,
      fillColor: palette.fillColor,
      fillOpacity: 0.65,
    });

    const popup = `
      <div class="popup-title">${p.stop_code}</div>
      <div class="popup-row">Comuna: ${p.comuna}</div>
      <div class="popup-row">Modo: ${p.mode_code}</div>
      <div class="popup-row">Tipo dia: ${p.tipo_dia}</div>
      <div class="popup-row">Hora: ${p.hour_of_day ?? "-"}:00</div>
      <div class="popup-row">Fecha: ${p.service_date}</div>
      <div class="popup-row">Etapas estimadas: ${formatValue(p.etapas_estimadas, "etapas_estimadas")}</div>
    `;
    circle.bindPopup(popup);
    circle.on("click", () => {
      selectedRowKey = pointKey(p);
      applySelectedRowStyle();
      scrollToSelectedRow();
      setStreetView(lat, lon, p.stop_code, Boolean(p.approx_location));
    });
    circle.addTo(mapLayer);
    markerByPointKey.set(pointKey(p), circle);
    bounds.push([lat, lon]);
  });

  mapPointCount.textContent = pointCountLabel || `${points.length} puntos`;
  if (bounds.length > 0) {
    map.fitBounds(bounds, { padding: [25, 25], maxZoom: 13 });
  }

  const first = points[0];
  if (first && Number.isFinite(Number(first.lat)) && Number.isFinite(Number(first.lon))) {
    setStreetView(Number(first.lat), Number(first.lon), first.stop_code, Boolean(first.approx_location));
  }

  if (selectedRowKey) {
    focusMapByRowKey(selectedRowKey);
  }
}

async function refreshMap(payload) {
  try {
    const requestedLimit = Math.max(Number(payload.limit || 20), 1);
    const mapLimit = payload.query_type === "top_boardings"
      ? 2000
      : 300;

    const body = {
      cut_from: payload.cut_from,
      cut_to: payload.cut_to,
      tipo_dia: payload.tipo_dia,
      mode: payload.mode,
      hour_from: payload.hour_from,
      hour_to: payload.hour_to,
      limit: mapLimit,
    };

    const res = await fetch("/api/map_points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      renderMap([]);
      return;
    }
    const data = await res.json();
    let points = data.points || [];

    // For top_boardings, render only exact stop+mode matches.
    if (payload.query_type === "top_boardings" && lastRows.length > 0) {
      const topKeys = new Set(
        lastRows
          .slice(0, requestedLimit)
          .map((r) => `${normalizeKey(r.stop_code)}|${normalizeKey(r.mode_code)}`),
      );
      points = points.filter((p) => topKeys.has(`${normalizeKey(p.stop_code)}|${normalizeKey(p.mode_code)}`));
      points = points.slice(0, requestedLimit);
    }

    if (selectedRowKey && !points.some((p) => pointKey(p) === selectedRowKey)) {
      selectedRowKey = null;
    }

    if (geoCoverage) {
      if (payload.query_type === "top_boardings") {
        const topTotal = Math.min(requestedLimit, lastRows.length);
        geoCoverage.textContent = `Cobertura geográfica exacta: ${points.length}/${topTotal} paraderos`;
      } else {
        geoCoverage.textContent = `Cobertura geográfica: ${points.length} puntos visibles`;
      }
    }

    const label = payload.query_type === "top_boardings"
      ? `${points.length}/${Math.min(requestedLimit, lastRows.length)} puntos exactos`
      : null;
    renderMap(points, label);
  } catch (_err) {
    renderMap([]);
  }
}

function buildInsights(queryType, rows) {
  if (!rows.length) {
    return ["No se encontraron filas con los filtros actuales.", "Prueba ampliando el rango de fechas o activando mas modos."];
  }

  if (queryType === "overview") {
    const r = rows[0];
    const estViajes = r.viajes_estimados || 0;
    const estEtapas = r.etapas_estimadas || 0;
    const ratio = estViajes > 0 ? (estEtapas / estViajes) : 0;
    return [
      `Se estiman ${formatValue(estViajes, "viajes_estimados")} viajes y ${formatValue(estEtapas, "etapas_estimadas")} etapas para el filtro actual.`,
      "Las metricas estimadas usan factores de expansion: pueden ser decimales y siguen siendo validas estadisticamente.",
      `Relacion etapas/viaje estimada: ${formatValue(ratio)}.`,
      "Usa Demanda por Modo para identificar donde concentrar medidas operacionales.",
    ];
  }

  if (queryType === "demand_by_mode") {
    const top = rows[0];
    const total = rows.reduce((acc, row) => acc + (row.etapas_estimadas || 0), 0);
    const share = total > 0 ? ((top.etapas_estimadas || 0) / total) * 100 : 0;
    return [
      `Modo lider: ${top.mode_code} con ${formatValue(top.etapas_estimadas, "etapas_estimadas")} etapas estimadas.`,
      `Participacion aproximada del modo lider: ${formatValue(share)}%.`,
      "Compara con Tipo de Dia para evaluar cambios de demanda en fines de semana.",
    ];
  }

  if (queryType === "demand_by_day_type") {
    const top = rows[0];
    return [
      `Tipo de dia dominante: ${top.tipo_dia} con ${formatValue(top.etapas_estimadas, "etapas_estimadas")} etapas estimadas.`,
      "Revisa brechas entre LABORAL, SABADO y DOMINGO para ajustar frecuencias.",
    ];
  }

  if (queryType === "top_boardings") {
    const top = rows[0];
    return [
      `Paradero mas cargado: ${top.stop_code} (${top.comuna}) con ${formatValue(top.subidas_promedio_total, "subidas_promedio_total")} subidas promedio.`,
      `Top solicitado: ${rows.length} fila(s) devueltas. Si pediste mas de las disponibles, se mostraran solo las existentes.`,
      "El Top 5 ayuda a priorizar supervision y recursos en terreno.",
    ];
  }

  return [];
}

function renderKpis(rows) {
  kpiStrip.innerHTML = "";
  if (!rows.length) return;

  const first = rows[0];
  const numeric = Object.entries(first)
    .filter(([, v]) => typeof v === "number")
    .slice(0, 4);

  numeric.forEach(([name, value]) => {
    const card = document.createElement("div");
    card.className = "kpi";
    card.innerHTML = `<div class="name">${getColLabel(name)}</div><div class="value">${formatValue(value, name)}</div>`;
    kpiStrip.appendChild(card);
  });
}

function toCsv(rows) {
  if (!rows.length) return "";
  const cols = Object.keys(rows[0]);
  const head = cols.join(",");
  const body = rows
    .map((row) => cols.map((c) => JSON.stringify(row[c] ?? "")).join(","))
    .join("\n");
  return `${head}\n${body}`;
}

async function checkHealth() {
  try {
    const res = await fetch("/api/health");
    const data = await res.json();
    if (data.data_ready) {
      healthBadge.className = "health ok";
      healthBadge.textContent = "Datos listos para consultar";
    } else {
      healthBadge.className = "health fail";
      healthBadge.textContent = "Sin datos Silver disponibles";
    }
  } catch (_err) {
    healthBadge.className = "health fail";
    healthBadge.textContent = "No se pudo conectar con la API";
  }
}

async function runQuery() {
  runBtn.disabled = true;
  runBtn.textContent = "Consultando...";
  queryMeta.textContent = "Consultando...";

  const payload = {
    query_type: queryTypeEl.value,
    cut_from: cutFromEl.value || null,
    cut_to: cutToEl.value || null,
    tipo_dia: selectedValues("tipoDiaChips"),
    mode: selectedValues("modeChips"),
    hour_from: clampHour(hourFromEl?.value, 0),
    hour_to: clampHour(hourToEl?.value, 23),
    limit: Number(limitEl.value || 20),
  };

  if (payload.hour_from > payload.hour_to) {
    alert("La hora desde no puede ser mayor que la hora hasta.");
    runBtn.disabled = false;
    runBtn.textContent = "Ejecutar Consulta";
    queryMeta.textContent = "Rango horario invalido.";
    return;
  }

  try {
    const started = performance.now();
    const res = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const msg = await res.text();
      throw new Error(msg);
    }

    const data = await res.json();
    const elapsedMs = Math.round(performance.now() - started);
    lastRows = data.rows || [];
    renderKpis(lastRows);
    renderTable(lastRows);
    setInsights(buildInsights(payload.query_type, lastRows));
    if (lastRows.length === 0) {
      renderMap([]);
    } else {
      await refreshMap(payload);
    }
    const limitNote = payload.query_type === "top_boardings"
      ? ` | Top solicitado: ${payload.limit}`
      : " | Nota: Limite solo aplica a Top Paraderos";
    queryMeta.textContent = `Ultima consulta: ${new Date().toLocaleString("es-CL")} | Tiempo de respuesta: ${elapsedMs} ms | Hora: ${payload.hour_from}:00-${payload.hour_to}:59${limitNote}`;
  } catch (err) {
    alert(`Error de consulta: ${err.message}`);
    renderMap([]);
    queryMeta.textContent = "La consulta fallo. Revisa filtros o disponibilidad de datos.";
  } finally {
    runBtn.disabled = false;
    runBtn.textContent = "Ejecutar Consulta";
  }
}

function downloadCsv() {
  const csv = toCsv(lastRows);
  if (!csv) {
    alert("No hay resultados para exportar.");
    return;
  }
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `consulta_${Date.now()}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

bindChips("tipoDiaChips");
bindChips("modeChips");
runBtn.addEventListener("click", runQuery);
downloadBtn.addEventListener("click", downloadCsv);

checkHealth();
initMap();
runQuery();
