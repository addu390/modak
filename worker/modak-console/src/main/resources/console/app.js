/* Modak console: polls the worker's JSON API and renders the fleet. */
"use strict";

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

const PALETTE = ["#f5a623", "#3fb8af", "#e05c6e", "#62c073", "#9a7fd1", "#5b9bd5"];

echarts.registerTheme("modak", {
  color: PALETTE,
  backgroundColor: "transparent",
  textStyle: { color: cssVar("--text-dim"), fontSize: 11 },
  legend: { textStyle: { color: cssVar("--text-dim"), fontSize: 11 } },
  categoryAxis: { axisLine: { lineStyle: { color: cssVar("--line") } } },
});

const charts = {};

function chart(id) {
  if (!charts[id]) {
    charts[id] = echarts.init(document.getElementById(id), "modak");
  }
  return charts[id];
}

window.addEventListener("resize", () => Object.values(charts).forEach(c => c.resize()));

function lineOption(seriesMap, formatter) {
  const names = Object.keys(seriesMap).sort();
  return {
    animation: false,
    grid: { left: 56, right: 16, top: names.length > 1 ? 34 : 12, bottom: 24 },
    legend: names.length > 1 ? { top: 0, icon: "roundRect", itemWidth: 10, itemHeight: 4 } : undefined,
    tooltip: {
      trigger: "axis",
      backgroundColor: cssVar("--bg-raised"),
      borderColor: cssVar("--line"),
      textStyle: { color: cssVar("--text"), fontSize: 12 },
      valueFormatter: formatter,
    },
    xAxis: {
      type: "time",
      axisLine: { lineStyle: { color: cssVar("--line") } },
      axisLabel: { color: cssVar("--text-dim"), hideOverlap: true },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: cssVar("--text-dim"), formatter },
      splitLine: { lineStyle: { color: cssVar("--bg-hover") } },
    },
    series: names.map(name => ({
      name,
      type: "line",
      showSymbol: false,
      smooth: 0.2,
      smoothMonotone: "x",
      lineStyle: { width: 1.6 },
      areaStyle: names.length === 1 ? { opacity: 0.08 } : undefined,
      data: seriesMap[name].map(p => [p[0] * 1000, p[1]]),
    })),
  };
}

function fmtBytes(v) {
  if (v == null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  let n = Number(v);
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + " " + units[i];
}

function fmtNum(v) {
  if (v == null || Number(v) < -9e18) return "—"; // Long.MIN sentinel on mirrored cutlines
  return Number(v).toLocaleString("en-US");
}

function fmtAgo(epoch) {
  if (!epoch) return "—";
  const s = Math.max(0, Math.floor(Date.now() / 1000) - epoch);
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, c => "&#" + c.charCodeAt(0) + ";");
}

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(url + " -> " + res.status);
  return res.json();
}

function groupSeries(all, prefix, only) {
  const out = {};
  for (const [key, points] of Object.entries(all)) {
    const [kind, label] = key.split("|");
    if (kind !== prefix) continue;
    if (only && label !== only) continue;
    out[label] = points;
  }
  return out;
}

/* ---------- overview ---------- */

let lastOverview = null;

function stateBar(states) {
  if (!states) return '<span class="dim">—</span>';
  const order = ["hot", "sealing", "tiering", "tiered", "dropped"];
  return '<span class="state-bar">' + order
    .filter(s => states[s])
    .map(s => `<span class="seg ${s}">${states[s]} ${s}</span>`)
    .join("") + "</span>";
}

function renderOverview(o) {
  lastOverview = o;
  document.getElementById("pill-role").textContent = o.leader ? "leader" : "standby";
  document.getElementById("pill-role").className = "pill " + (o.leader ? "ok" : "warn");
  document.getElementById("pill-schema").textContent = "schema v" + o.schemaVersion;
  document.getElementById("pill-wal").textContent = "wal " + fmtBytes(o.walLsn);
  const live = document.getElementById("pill-live");
  live.textContent = "live";
  live.className = "pill ok";

  const mirrored = o.tables.filter(t => t.mode === "mirrored");
  const copying = o.tables.filter(t => t.copying);
  const backlog = o.tables.reduce((n, t) => n + t.deltaBacklog, 0);
  const pins = o.tables.reduce((n, t) => n + t.readPins, 0);
  const maxLag = Math.max(0, ...mirrored.map(t => t.lagBytes || 0));
  const inactive = o.slots.filter(s => !s.active).length;

  document.getElementById("cards").innerHTML = [
    card("tables", fmtNum(o.tables.length),
      `${o.tables.length - mirrored.length} tiered · ${mirrored.length} mirrored`, true),
    card("initial copies", fmtNum(copying.length),
      copying.length ? copying.map(t => t.name).join(", ") : "none in flight"),
    card("delta backlog", fmtNum(backlog), "rows awaiting compaction"),
    card("read pins", fmtNum(pins), "active pinned readers"),
    card("worst mirror lag", fmtBytes(maxLag), mirrored.length ? "behind current WAL" : "no mirrored tables"),
    card("replication slots", fmtNum(o.slots.length),
      inactive ? `${inactive} inactive` : "all active"),
  ].join("");

  const rows = o.tables.map(t => {
    const mode = t.copying
      ? `<span class="badge copying">copying · ${fmtNum(t.copyChunks)} chunks</span>`
      : `<span class="badge ${t.mode}">${t.mode}</span>`;
    const lag = t.mode === "mirrored" ? fmtBytes(t.lagBytes) : '<span class="dim">—</span>';
    return `<tr data-id="${t.id}">
      <td>${esc(t.schema)}.${esc(t.name)}</td>
      <td>${mode}</td>
      <td>${fmtNum(t.cutlineT)}</td>
      <td>${fmtNum(t.cutlineS)}</td>
      <td>${lag}</td>
      <td>${fmtNum(t.deltaBacklog)}</td>
      <td>${fmtNum(t.readPins)}</td>
      <td>${stateBar(t.partitions)}</td>
    </tr>`;
  }).join("");
  document.getElementById("fleet").innerHTML = rows;
  document.getElementById("fleet-empty").hidden = o.tables.length > 0;
  document.querySelectorAll("#fleet tr").forEach(tr =>
    tr.addEventListener("click", () => { location.hash = "#/table/" + tr.dataset.id; }));
}

function card(label, value, sub, accent) {
  return `<div class="card${accent ? " accent" : ""}">
    <div class="label">${label}</div>
    <div class="value">${value}</div>
    <div class="value"><small>${esc(sub)}</small></div>
  </div>`;
}

function renderOverviewCharts(all) {
  chart("chart-lag").setOption(lineOption(groupSeries(all, "mirror_lag_bytes"), fmtBytes), true);
  chart("chart-backlog").setOption(lineOption(groupSeries(all, "delta_backlog"), fmtNum), true);
  chart("chart-slots").setOption(lineOption(groupSeries(all, "slot_wal_bytes"), fmtBytes), true);
  chart("chart-commits").setOption(lineOption(groupSeries(all, "lake_commits"), fmtNum), true);
}

/* ---------- table detail ---------- */

function renderTable(t) {
  document.getElementById("detail-name").textContent = t.schema + "." + t.name;
  const mode = document.getElementById("detail-mode");
  mode.textContent = t.mode;
  mode.className = "badge " + t.mode;

  const fleet = (lastOverview?.tables || []).find(x => x.id === t.id) || {};
  const slot = (lastOverview?.slots || []).find(s => s.name === t.slot);
  const cells = [
    card("tier key", esc(t.tierKey), "primary key: " + t.pk.join(", ")),
    card("cutline T", fmtNum(fleet.cutlineT), "snapshot S: " + fmtNum(fleet.cutlineS), true),
    card("delta backlog", fmtNum(fleet.deltaBacklog), "read pins: " + fmtNum(fleet.readPins)),
    card("lake", esc(t.lakeFormat), t.lakeRef),
  ];
  if (t.mode === "mirrored") {
    cells.push(card("mirror lag", fmtBytes(fleet.lagBytes),
      fleet.copying ? "initial copy · " + fmtNum(fleet.copyChunks) + " chunks" : "streaming"));
    if (slot) {
      cells.push(card("slot", slot.active ? "active" : "inactive",
        fmtBytes(slot.retainedBytes) + " WAL retained"));
    }
  }
  if (t.heapRetentionLag != null) {
    cells.push(card("heap retention", fmtNum(t.heapRetentionLag), "heap partitions kept behind highwater"));
  }
  document.getElementById("detail-meta").innerHTML = cells.join("");

  document.getElementById("detail-partitions").innerHTML = t.partitions.map(p => `<tr>
    <td>${esc(p.id)}</td>
    <td class="dim">[${fmtNum(p.lo)}, ${fmtNum(p.hi)})</td>
    <td><span class="seg ${p.state}">${p.state}</span></td>
    <td class="dim">${fmtAgo(p.updatedAt)}</td>
  </tr>`).join("");
  document.getElementById("partitions-empty").hidden = t.partitions.length > 0;

  document.getElementById("detail-ops").innerHTML = t.ops.map(op => `<tr>
    <td>${esc(op.kind)}</td>
    <td>${esc(op.phase)}</td>
    <td>${fmtNum(op.snapshot)}</td>
    <td class="dim">${esc(op.details ? JSON.stringify(op.details) : "—")}</td>
    <td class="dim">${fmtAgo(op.updatedAt)}</td>
  </tr>`).join("");
  document.getElementById("ops-empty").hidden = t.ops.length > 0;
}

function renderTableCharts(all, name) {
  const merged = {};
  const lag = groupSeries(all, "mirror_lag_bytes", name)[name];
  const backlog = groupSeries(all, "delta_backlog", name)[name];
  if (lag) merged["mirror lag (bytes)"] = lag;
  if (backlog) merged["delta backlog (rows)"] = backlog;
  chart("chart-detail").setOption(lineOption(merged, fmtNum), true);
  chart("chart-detail-commits").setOption(
    lineOption(groupSeries(all, "lake_commits", name), fmtNum), true);
}

/* ---------- playground ---------- */

const SNIPPETS = [
  ["Fleet status", "SELECT * FROM modak.status;"],
  ["Partition lifecycle", "SELECT * FROM modak.partitions ORDER BY table_id, tier_key_lo;"],
  ["Delta backlog", "SELECT t.schema_name || '.' || t.table_name AS tbl, count(d.*) AS backlog\nFROM modak.tables t LEFT JOIN modak.delta d USING (table_id)\nGROUP BY 1 ORDER BY 2 DESC;"],
  ["Recent operations", "SELECT op_kind, phase, lake_snapshot_id, details, updated_at\nFROM modak.tiering_log ORDER BY updated_at DESC LIMIT 20;"],
  ["Replication slots", "SELECT slot_name, active, pg_size_pretty(\n  pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained\nFROM pg_replication_slots WHERE slot_name LIKE 'modak\\_%';"],
  ["Create a demo table", "CREATE TABLE public.readings (\n  id bigint PRIMARY KEY,\n  reading_time bigint NOT NULL,\n  value double precision\n);\n-- then: docker compose run --rm worker register \\\n--   --table public.readings --pk id --tier-key reading_time"],
];

let editor = null;
let sqlBusy = false;

function initPlayground() {
  if (editor) return;
  editor = CodeMirror.fromTextArea(document.getElementById("sql-input"), {
    mode: "text/x-pgsql",
    lineNumbers: true,
    indentWithTabs: false,
    indentUnit: 2,
    extraKeys: {
      "Cmd-Enter": runSql,
      "Ctrl-Enter": runSql,
    },
  });
  editor.setValue("SELECT * FROM modak.status;");

  const snippets = document.getElementById("snippets");
  SNIPPETS.forEach(([name], i) => snippets.add(new Option(name, i)));
  snippets.addEventListener("change", () => {
    if (snippets.value !== "") editor.setValue(SNIPPETS[Number(snippets.value)][1]);
    snippets.value = "";
    editor.focus();
  });

  document.getElementById("run-btn").addEventListener("click", runSql);
  document.getElementById("history").addEventListener("change", e => {
    if (e.target.value !== "") editor.setValue(histLoad()[Number(e.target.value)].sql);
    e.target.value = "";
    editor.focus();
  });
  renderHistory();
}

function histLoad() {
  try { return JSON.parse(localStorage.getItem("modak.sqlHistory")) || []; }
  catch (e) { return []; }
}

function histPush(sql) {
  const hist = histLoad().filter(h => h.sql !== sql);
  hist.unshift({ sql, ts: Date.now() });
  localStorage.setItem("modak.sqlHistory", JSON.stringify(hist.slice(0, 50)));
  renderHistory();
}

function renderHistory() {
  const sel = document.getElementById("history");
  while (sel.options.length > 1) sel.remove(1);
  histLoad().forEach((h, i) =>
    sel.add(new Option(h.sql.replace(/\s+/g, " ").slice(0, 60), i)));
}

async function runSql() {
  if (sqlBusy || !editor) return;
  const sql = (editor.somethingSelected() ? editor.getSelection() : editor.getValue()).trim();
  if (!sql) return;
  sqlBusy = true;
  const status = document.getElementById("sql-status");
  status.textContent = "running…";
  status.className = "sql-status";
  try {
    const res = await fetch("/api/query", { method: "POST", body: sql });
    const out = await res.json();
    renderResults(out);
    if (!out.error) histPush(sql);
  } catch (e) {
    renderResults({ error: String(e) });
  } finally {
    sqlBusy = false;
  }
}

function renderResults(out) {
  const box = document.getElementById("sql-results");
  const title = document.getElementById("results-title");
  const status = document.getElementById("sql-status");
  if (out.error) {
    title.textContent = "Results";
    box.innerHTML = `<div class="sql-error">${esc(out.error)}</div>`;
    status.textContent = out.elapsedMs != null ? out.elapsedMs + " ms" : "";
    status.className = "sql-status err";
    return;
  }
  status.className = "sql-status";
  status.textContent = out.elapsedMs + " ms";
  if (out.columns) {
    title.textContent = `Results — ${fmtNum(out.rowCount)} row(s)`
      + (out.truncated ? ` (showing first ${fmtNum(out.rowCount)})` : "");
    const head = out.columns.map(c =>
      `<th>${esc(c.name)} <span class="type">${esc(c.type)}</span></th>`).join("");
    const body = out.rows.map(r => "<tr>" + r.map(v =>
      v === null ? '<td class="null">null</td>' : `<td>${esc(v)}</td>`).join("") + "</tr>").join("");
    box.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  } else {
    title.textContent = "Results";
    box.innerHTML = `<div class="empty">OK — ${fmtNum(out.updateCount)} row(s) affected.</div>`;
  }
}

async function loadSchema() {
  try {
    const { tables } = await getJson("/api/schema");
    document.getElementById("schema-tree").innerHTML = tables.map((t, i) =>
      `<div class="schema-table" data-i="${i}">
        <div class="name">${esc(t.name)}</div>
        <div class="cols">${t.columns.map(c =>
          `<div class="col">${esc(c.name)} <span class="type">${esc(c.type)}</span></div>`).join("")}</div>
      </div>`).join("");
    document.querySelectorAll(".schema-table > .name").forEach(el => {
      el.addEventListener("click", () => el.parentElement.classList.toggle("open"));
      el.addEventListener("dblclick", () => {
        editor.replaceSelection(el.textContent);
        editor.focus();
      });
    });
  } catch (e) {
    document.getElementById("schema-tree").innerHTML = '<div class="empty">schema unavailable</div>';
  }
}

/* ---------- routing + polling ---------- */

function currentTableId() {
  const m = location.hash.match(/^#\/table\/(\d+)$/);
  return m ? Number(m[1]) : null;
}

function currentView() {
  if (location.hash === "#/sql") return "sql";
  return currentTableId() !== null ? "table" : "overview";
}

function showView(view) {
  document.getElementById("view-overview").hidden = view !== "overview";
  document.getElementById("view-table").hidden = view !== "table";
  document.getElementById("view-sql").hidden = view !== "sql";
  document.getElementById("tab-overview").className = view !== "sql" ? "active" : "";
  document.getElementById("tab-sql").className = view === "sql" ? "active" : "";
  if (view === "sql") {
    initPlayground();
    loadSchema();
    editor.refresh();
  }
}

async function tick() {
  const view = currentView();
  showView(view);
  try {
    const overview = await getJson("/api/overview");
    renderOverview(overview);
    if (view === "table") {
      const t = await getJson("/api/table?id=" + currentTableId());
      renderTable(t);
    }
  } catch (e) {
    const live = document.getElementById("pill-live");
    live.textContent = "offline";
    live.className = "pill err";
  }
}

async function tickSeries() {
  const view = currentView();
  if (view === "sql") return;
  try {
    const { series } = await getJson("/api/series");
    if (view === "overview") {
      renderOverviewCharts(series);
    } else {
      const t = (lastOverview?.tables || []).find(x => x.id === currentTableId());
      if (t) renderTableCharts(series, t.schema + "." + t.name);
    }
  } catch (e) { /* offline pill handled by tick() */ }
}

window.addEventListener("hashchange", () => {
  tick().then(tickSeries).then(() => Object.values(charts).forEach(c => c.resize()));
});
tick().then(tickSeries);
setInterval(tick, 3000);
setInterval(tickSeries, 10000);
