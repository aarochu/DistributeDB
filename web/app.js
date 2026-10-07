/* DistributeDB dashboard. Every value shown comes from the bridge, which
 * reads it from the nodes' STATS or from a proxied command. Nothing here
 * fabricates samples: until two polls exist, rate charts say so. */
(function () {
  "use strict";

  const POLL_MS = 1000;
  const HISTORY_MAX = 300;
  const $ = (id) => document.getElementById(id);

  const app = {
    state: null, // last /api/state
    model: null, // derived from state
    history: [], // {t, nodes: [stats|null]}
    nodeIds: {}, // node name -> node_id, remembered while a node is down
    lastOk: 0,
    bridgeDown: false,
    malformed: false,
    builtFor: "", // which interactive panels have been built
    topology: null,
  };

  // --- formatting -----------------------------------------------------------
  function esc(text) {
    return String(text).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  }
  function num(value) {
    if (value === undefined || value === null || value === "" || value === "unknown") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  const dash = "—";
  function fmtInt(n) {
    return n === null || n === undefined || Number.isNaN(n) ? dash : Math.round(n).toLocaleString("en-US");
  }
  function fmtMs(us) {
    if (us === null || us === undefined) return dash;
    const ms = us / 1000;
    return (ms >= 100 ? ms.toFixed(0) : ms >= 10 ? ms.toFixed(1) : ms.toFixed(2)) + " ms";
  }
  function fmtBytes(n) {
    if (n === null || n === undefined) return dash;
    const units = ["B", "KiB", "MiB", "GiB"];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return (i === 0 ? String(v) : v.toFixed(1)) + " " + units[i];
  }
  function fmtUptime(s) {
    if (s === null) return dash;
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = Math.floor(s % 60);
    if (h > 0) return `${h}h ${String(m).padStart(2, "0")}m`;
    if (m > 0) return `${m}m ${String(sec).padStart(2, "0")}s`;
    return `${sec}s`;
  }
  function fmtRate(r) {
    if (r === null || r === undefined) return dash;
    return r < 10 ? r.toFixed(1) : Math.round(r).toLocaleString("en-US");
  }

  // --- bytes ----------------------------------------------------------------
  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  function hexToBytes(hex) {
    const clean = hex.replace(/\s+/g, "");
    if (clean.length % 2 !== 0 || /[^0-9a-fA-F]/.test(clean)) return null;
    const out = new Uint8Array(clean.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.substr(i * 2, 2), 16);
    return out;
  }
  function bytesToHex(bytes) {
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  }
  function utf8(bytes) {
    try {
      return decoder.decode(bytes);
    } catch (_) {
      return null;
    }
  }
  function printable(text) {
    return text !== null && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text);
  }
  function displayBytes(bytes) {
    const text = utf8(bytes);
    if (printable(text) && bytes.length > 0) return { text, hex: false };
    return { text: bytes.length ? "0x" + bytesToHex(bytes) : "(empty)", hex: true };
  }
  function hexDump(bytes) {
    const rows = [];
    for (let offset = 0; offset < bytes.length; offset += 16) {
      const slice = bytes.slice(offset, offset + 16);
      const hex = Array.from(slice, (b) => b.toString(16).toUpperCase().padStart(2, "0"));
      const left = hex.slice(0, 8).join(" ");
      const right = hex.slice(8).join(" ");
      const ascii = Array.from(slice, (b) => (b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : ".")).join("");
      rows.push(`<div class="hex-row"><span class="off">${offset.toString(16).padStart(8, "0")}</span><span>${esc((left + "  " + right).padEnd(49))}</span><span class="ascii">${esc(ascii)}</span></div>`);
    }
    return rows.join("") || '<span class="placeholder">(empty value)</span>';
  }

  // --- bridge API -----------------------------------------------------------
  class BridgeError extends Error {
    constructor(kind, message, status) {
      super(message);
      this.kind = kind;
      this.status = status;
    }
  }
  async function api(path, options) {
    let response;
    try {
      response = await fetch(path, Object.assign({ cache: "no-store" }, options));
    } catch (_) {
      throw new BridgeError("bridge", "bridge unavailable");
    }
    let body;
    try {
      body = await response.json();
    } catch (_) {
      throw new BridgeError("malformed", "malformed response from " + path);
    }
    if (!response.ok) throw new BridgeError("http", body && body.error ? body.error : "HTTP " + response.status, response.status);
    return body;
  }
  function post(path, body) {
    return api(path, { method: "POST", headers: { "x-ddb-request": "1", "content-type": "text/plain" }, body: body || "" });
  }
  function errorLine(error) {
    if (error.kind === "malformed") return "MALFORMED RESPONSE";
    if (error.kind === "bridge") return "BRIDGE UNAVAILABLE";
    if (error.status === 502) return "NODE UNAVAILABLE: " + error.message;
    return error.message;
  }

  // --- derived model ----------------------------------------------------------
  function derive(state) {
    state.nodes.forEach((n) => {
      if (n.up && n.stats.node_id) app.nodeIds[n.name] = n.stats.node_id;
    });
    const primary = state.nodes[0];
    const p = primary.up ? primary.stats : null;
    const replicas = state.nodes.slice(1).map((n) => {
      const id = app.nodeIds[n.name];
      const applied = p && id ? num(p[`replica_${id}_applied_lsn`]) : null;
      const lagRaw = p && id ? p[`replica_${id}_lag`] : undefined;
      let state = "disconnected";
      let lag = null;
      let detail = "";
      if (!n.up) {
        detail = "node unavailable";
      } else if (!p) {
        state = "online";
        detail = "primary unavailable; lag unknown";
      } else if (lagRaw === undefined) {
        detail = "reachable; not yet streaming from the primary";
      } else if (lagRaw === "unknown") {
        detail = "reachable; replication session closed";
      } else {
        lag = num(lagRaw);
        state = lag === 0 ? "synced" : "lagging";
      }
      return { name: n.name, addr: n.addr, up: n.up, error: n.error, s: n.up ? n.stats : null, id, applied, lag, state, detail };
    });
    const connected = p ? num(p.replicas_connected) : null;
    return { primary, p, replicas, connected };
  }

  // --- charts -----------------------------------------------------------------
  const DASHES = [[], [5, 4], [1, 3], [8, 3, 1, 3]];

  function clearChart(canvas) {
    canvas._spec = null;
    canvas.getContext("2d").clearRect(0, 0, canvas.width, canvas.height);
  }

  function drawChart(canvas, spec) {
    canvas._spec = spec;
    const cssHeight = Number(canvas.dataset.h || canvas.getAttribute("height"));
    canvas.dataset.h = cssHeight;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const width = canvas.clientWidth;
    canvas.style.height = cssHeight + "px";
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(cssHeight * dpr);
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, cssHeight);
    const pad = { left: 72, right: 104, top: 10, bottom: 22 };
    const plotW = Math.max(width - pad.left - pad.right, 10);
    const plotH = cssHeight - pad.top - pad.bottom;
    const values = [];
    spec.series.forEach((s) => s.points.forEach(([, v]) => v !== null && values.push(v)));
    const times = spec.series.flatMap((s) => s.points.map(([t]) => t));
    if (values.length < 2 || times.length < 2) {
      spec.layout = null;
      return false;
    }
    let lo = Math.min(...values);
    let hi = Math.max(...values);
    if (spec.zero) lo = Math.min(lo, 0);
    if (hi === lo) {
      hi += 1;
      lo = spec.zero ? lo : lo - 1;
    }
    const margin = (hi - lo) * 0.08;
    hi += margin;
    if (!spec.zero) lo = values.every((v) => v >= 0) ? Math.max(lo - margin, 0) : lo - margin;
    const t0 = Math.min(...times);
    const t1 = Math.max(...times);
    const x = (t) => pad.left + ((t - t0) / Math.max(t1 - t0, 1)) * plotW;
    const y = (v) => pad.top + (1 - (v - lo) / (hi - lo)) * plotH;
    spec.layout = { t0, t1, pad, plotW };

    ctx.font = "11px " + getComputedStyle(document.body).getPropertyValue("--mono");
    ctx.textBaseline = "middle";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const v = lo + ((hi - lo) * i) / 4;
      const yy = Math.round(y(v)) + 0.5;
      ctx.strokeStyle = "#191919";
      ctx.beginPath();
      ctx.moveTo(pad.left, yy);
      ctx.lineTo(pad.left + plotW, yy);
      ctx.stroke();
      ctx.fillStyle = "#666666";
      ctx.textAlign = "right";
      ctx.fillText(spec.format(v), pad.left - 8, yy);
    }
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const span = (t1 - t0) / 1000;
    [0, 0.5, 1].forEach((f) => {
      const t = t0 + (t1 - t0) * f;
      const ago = Math.round((t1 - t) / 1000);
      const label = ago === 0 ? "now" : `−${ago >= 60 && span >= 120 ? Math.round(ago / 60) + "m" : ago + "s"}`;
      ctx.fillStyle = "#666666";
      ctx.fillText(label, x(t), pad.top + plotH + 6);
    });

    const ends = [];
    spec.series.forEach((s) => {
      ctx.strokeStyle = `rgba(255,255,255,${s.lum})`;
      ctx.lineWidth = s.width;
      ctx.setLineDash(s.dash || []);
      ctx.beginPath();
      let prev = null;
      s.points.forEach(([t, v]) => {
        if (v === null) {
          prev = null;
          return;
        }
        if (prev === null) ctx.moveTo(x(t), y(v));
        else if (spec.stepped) {
          ctx.lineTo(x(t), y(prev));
          ctx.lineTo(x(t), y(v));
        } else ctx.lineTo(x(t), y(v));
        prev = v;
      });
      ctx.stroke();
      ctx.setLineDash([]);
      const last = [...s.points].reverse().find(([, v]) => v !== null);
      if (last) ends.push({ y: y(last[1]), label: s.label, lum: s.lum });
    });
    ends.sort((a, b) => a.y - b.y);
    for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + 13);
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ends.forEach((e) => {
      ctx.fillStyle = `rgba(255,255,255,${Math.max(e.lum, 0.55)})`;
      ctx.fillText(e.label, pad.left + plotW + 8, e.y);
    });

    if (canvas._hover !== undefined && canvas._hover !== null) {
      const hx = Math.min(Math.max(canvas._hover, pad.left), pad.left + plotW);
      ctx.strokeStyle = "#3a3a3a";
      ctx.beginPath();
      ctx.moveTo(Math.round(hx) + 0.5, pad.top);
      ctx.lineTo(Math.round(hx) + 0.5, pad.top + plotH);
      ctx.stroke();
    }
    return true;
  }

  function valueAt(points, t) {
    let found = null;
    for (const [pt, v] of points) {
      if (pt <= t) found = v;
      else break;
    }
    return found;
  }

  function attachHover(canvas) {
    const tooltip = $("tooltip");
    canvas.addEventListener("mousemove", (event) => {
      const spec = canvas._spec;
      if (!spec || !spec.layout) return;
      const rect = canvas.getBoundingClientRect();
      canvas._hover = event.clientX - rect.left;
      drawChart(canvas, spec);
      const { t0, t1, pad, plotW } = spec.layout;
      const frac = Math.min(Math.max((canvas._hover - pad.left) / plotW, 0), 1);
      const t = t0 + (t1 - t0) * frac;
      const ago = ((t1 - t) / 1000).toFixed(0);
      const lines = [ago === "0" ? "now" : `−${ago} s`].concat(spec.series.map((s) => `${s.label.padEnd(15)} ${spec.format(valueAt(s.points, t))}`));
      tooltip.textContent = lines.join("\n");
      tooltip.hidden = false;
      tooltip.style.left = Math.min(event.clientX + 14, window.innerWidth - tooltip.offsetWidth - 8) + "px";
      tooltip.style.top = event.clientY + 14 + "px";
    });
    canvas.addEventListener("mouseleave", () => {
      canvas._hover = null;
      tooltip.hidden = true;
      if (canvas._spec) drawChart(canvas, canvas._spec);
    });
  }

  // --- history ----------------------------------------------------------------
  function ingest(state) {
    const t = state.nodes[0].observed_ms || state.time_ms;
    const last = app.history[app.history.length - 1];
    if (last && last.t >= t) return;
    app.history.push({ t, nodes: state.nodes.map((n) => (n.up ? n.stats : null)) });
    while (app.history.length > HISTORY_MAX) app.history.shift();
  }

  function primaryRates() {
    const rows = [];
    for (let i = 1; i < app.history.length; i++) {
      const a = app.history[i - 1].nodes[0];
      const b = app.history[i].nodes[0];
      if (!a || !b) continue;
      const dt = (app.history[i].t - app.history[i - 1].t) / 1000;
      const reads = (num(b.reads_total) - num(a.reads_total)) / dt;
      const writes = (num(b.writes_total) - num(a.writes_total)) / dt;
      // A restarted node resets its counters; skip that interval.
      if (!(dt > 0) || reads < 0 || writes < 0) continue;
      rows.push({ t: app.history[i].t, reads, writes });
    }
    return rows;
  }

  // --- rendering --------------------------------------------------------------
  function stateWord(state, extra) {
    const marks = { online: "■", synced: "●", healthy: "●", lagging: "◇", disconnected: "○", unavailable: "▲", error: "▲" };
    const mark = marks[state] ? marks[state] + " " : "";
    return `<span class="state ${state}">${mark}${esc(extra || state.toUpperCase())}</span>`;
  }

  function nodeRows(entries) {
    return `<dl class="kv">${entries.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
  }

  function renderHeader() {
    const health = $("bar-health");
    if (app.lastOk) $("bar-age").textContent = `polled ${((Date.now() - app.lastOk) / 1000).toFixed(1)} s ago`;
    if (app.bridgeDown || app.malformed) {
      health.className = "state unavailable";
      health.textContent = app.malformed ? "▲ MALFORMED RESPONSE" : "▲ BRIDGE UNAVAILABLE";
      $("bar-nodes").textContent = "— NODES";
      return;
    }
    const m = app.model;
    if (!m) return;
    const total = app.state.nodes.length;
    const up = app.state.nodes.filter((n) => n.up).length;
    $("bar-nodes").textContent = `${up} / ${total} NODES`;
    const disconnected = m.replicas.filter((r) => r.state === "disconnected").length;
    if (!m.primary.up) {
      health.className = "state unavailable";
      health.textContent = "▲ PRIMARY UNAVAILABLE";
    } else if (disconnected) {
      health.className = "state disconnected";
      health.textContent = `○ ${disconnected} DISCONNECTED`;
    } else if (m.replicas.some((r) => r.state === "lagging")) {
      health.className = "state lagging";
      health.textContent = "◇ LAGGING";
    } else {
      health.className = "state healthy";
      health.textContent = "● HEALTHY";
    }
  }

  function renderBanner() {
    const banner = $("banner");
    if (app.malformed) {
      banner.hidden = false;
      banner.textContent = "MALFORMED RESPONSE · /api/state did not return the expected JSON · retrying every 1 s";
    } else if (app.bridgeDown) {
      banner.hidden = false;
      banner.textContent = app.lastOk
        ? `BRIDGE UNAVAILABLE · last response ${((Date.now() - app.lastOk) / 1000).toFixed(0)} s ago · values below are stale · retrying every 1 s`
        : `BRIDGE UNAVAILABLE · no response from ${location.origin}/api/state · retrying every 1 s`;
    } else {
      banner.hidden = true;
    }
    document.body.classList.toggle("stale", (app.bridgeDown || app.malformed) && !!app.lastOk);
    if ((app.bridgeDown || app.malformed) && !app.lastOk) {
      ["tree", "storage-body", "console-body", "lab-body"].forEach((id) => {
        $(id).innerHTML = '<p class="placeholder">NO DATA</p>';
      });
      $("identity").innerHTML = "<dt>state</dt><dd>NO DATA</dd>";
    }
  }

  function renderIdentity() {
    const { p, primary, replicas } = app.model;
    const bridge = app.state.bridge;
    const rows = [];
    if (p) {
      const sync = num(p.sync_replicas);
      rows.push(["storage", esc(p.storage_engine || dash)]);
      rows.push(["durability", p.durability === "fsync" ? "fsync · group commit" : esc(p.durability) + " · benchmark mode, no sync"]);
      rows.push(["nodes", String(app.state.nodes.length)]);
      rows.push(["sync replicas", sync ? String(sync) : "0 · asynchronous"]);
    } else {
      rows.push(["primary", stateWord("unavailable", "NODE UNAVAILABLE")]);
    }
    rows.push(["primary addr", esc(primary.addr)]);
    rows.push(["replica addrs", replicas.length ? replicas.map((r) => esc(r.addr)).join(", ") : "none"]);
    rows.push(["bridge", `${esc(bridge.addr)} · writes ${bridge.writes ? "enabled" : "disabled"} · ${bridge.control ? "launcher control" : "no node control"}`]);
    $("identity").innerHTML = rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("");
  }

  function renderTree() {
    const { primary, p, replicas } = app.model;
    let primaryHtml;
    if (p) {
      primaryHtml = `<div class="node">
        <div class="label">Primary</div>
        <div class="node-head"><span class="node-name">${esc(primary.name)}</span>${stateWord("online")}</div>
        ${nodeRows([
          ["address", esc(primary.addr)],
          ["current lsn", fmtInt(num(p.current_lsn))],
          ["durable lsn", fmtInt(num(p.durable_lsn))],
          ["uptime", fmtUptime(num(p.uptime_seconds))],
          ["storage", esc(p.storage_engine)],
          ["durability", esc(p.durability)],
          ["node id", esc((p.node_id || dash).slice(0, 12))],
        ])}</div>`;
    } else {
      primaryHtml = `<div class="node down">
        <div class="label">Primary</div>
        <div class="node-head"><span class="node-name">${esc(primary.name)}</span>${stateWord("unavailable", "NODE UNAVAILABLE")}</div>
        ${nodeRows([["address", esc(primary.addr)], ["error", esc(primary.error || dash)]])}</div>`;
    }
    const replicaHtml = replicas
      .map((r) => {
        const rows = [["address", esc(r.addr)]];
        rows.push(["applied lsn", fmtInt(r.applied) + (r.applied !== null ? ' <span class="meta">acknowledged</span>' : "")]);
        if (r.state === "synced" || r.state === "lagging") rows.push(["lag", fmtInt(r.lag) + " entries"]);
        else if (p && r.applied !== null) rows.push(["behind", fmtInt(num(p.durable_lsn) - r.applied) + " entries since last ack"]);
        if (r.s) {
          rows.push(["local lsn", fmtInt(num(r.s.current_lsn))]);
          rows.push(["uptime", fmtUptime(num(r.s.uptime_seconds))]);
          rows.push(["storage", esc(r.s.storage_engine)]);
          rows.push(["durability", esc(r.s.durability)]);
        }
        if (r.detail) rows.push(["note", esc(r.detail)]);
        return `<div class="replica node ${r.state}${r.up ? "" : " down"}">
          <div class="label">Replica</div>
          <div class="node-head"><span class="node-name">${esc(r.name)}</span>${stateWord(r.state)}</div>
          ${nodeRows(rows)}</div>`;
      })
      .join("");
    $("tree").innerHTML = primaryHtml + `<div class="replicas">${replicaHtml || '<p class="placeholder">NO REPLICAS CONFIGURED</p>'}</div>`;
  }

  function renderSummary() {
    const { p, replicas, connected } = app.model;
    const rates = primaryRates();
    const latest = rates[rates.length - 1];
    const cells = [
      ["Ops/s", latest ? fmtRate(latest.reads + latest.writes) : "COLLECTING", ""],
      ["Read p99", p ? fmtMs(num(p.read_latency_p99_us)) : dash, ""],
      ["Write p99", p ? fmtMs(num(p.write_latency_p99_us)) : dash, ""],
      ["Durable LSN", p ? fmtInt(num(p.durable_lsn)) : dash, ""],
      ["Replicas", p ? `${fmtInt(connected)} / ${replicas.length}` : dash, "connected"],
      ["WAL sync avg", p ? fmtMs(num(p.wal_sync_avg_us)) : dash, ""],
    ];
    $("summary").innerHTML = cells
      .map(([label, value, unit]) => `<div><div class="label">${label}</div><div class="value">${esc(value)}${unit ? `<span class="unit">${unit}</span>` : ""}</div></div>`)
      .join("");
  }

  function renderReplication() {
    const { p, replicas, connected } = app.model;
    const rows = [`<tr><th>Node</th><th class="num">Position</th><th class="num">Lag</th><th>State</th></tr>`];
    rows.push(`<tr><td>primary <span class="meta">durable</span></td><td class="num">${p ? fmtInt(num(p.durable_lsn)) : dash}</td><td class="num">${dash}</td><td>${p ? stateWord("online") : stateWord("unavailable", "UNAVAILABLE")}</td></tr>`);
    replicas.forEach((r) => {
      rows.push(`<tr><td>${esc(r.name)} <span class="meta">applied</span></td><td class="num">${fmtInt(r.applied)}</td><td class="num">${r.lag === null ? "unknown" : fmtInt(r.lag)}</td><td>${stateWord(r.state)}</td></tr>`);
    });
    rows.push(`<tr><td class="text">connected</td><td class="num">${p ? `${fmtInt(connected)} / ${replicas.length}` : dash}</td><td></td><td></td></tr>`);
    $("repl-table").innerHTML = `<tbody>${rows.join("")}</tbody>`;

    const sync = p ? num(p.sync_replicas) : null;
    $("sync-block").innerHTML = sync
      ? `<h3>Synchronous acknowledgment</h3>${nodeRows([
          ["acks required", `${fmtInt(sync)} per write`],
          ["ack timeouts", fmtInt(num(p.sync_ack_timeouts_total)) + ' <span class="meta">answered UNAVAILABLE; durable on the primary</span>'],
        ])}`
      : "";

    const series = [
      { label: "primary", lum: 0.95, width: 1.5, dash: [], points: app.history.map((h) => [h.t, h.nodes[0] ? num(h.nodes[0].durable_lsn) : null]) },
    ];
    replicas.forEach((r, k) => {
      if (!r.id) return;
      series.push({
        label: r.name,
        lum: 0.6,
        width: 1,
        dash: DASHES[k % DASHES.length],
        points: app.history.map((h) => [h.t, h.nodes[0] ? num(h.nodes[0][`replica_${r.id}_applied_lsn`]) : null]),
      });
    });
    const drawn = drawChart($("chart-repl"), { series, stepped: true, format: (v) => (v === null ? dash : fmtInt(v)) });
    if (!drawn) clearChart($("chart-repl"));
    $("chart-repl-empty").hidden = drawn;
  }

  function renderTraffic() {
    const rates = primaryRates();
    const latest = rates[rates.length - 1];
    $("rates").innerHTML = [
      ["reads/s", latest ? fmtRate(latest.reads) : dash],
      ["writes/s", latest ? fmtRate(latest.writes) : dash],
      ["total ops/s", latest ? fmtRate(latest.reads + latest.writes) : dash],
    ]
      .map(([k, v]) => `<div><dt class="label">${k}</dt><dd>${v}</dd></div>`)
      .join("");
    const drawn =
      rates.length >= 2 &&
      drawChart($("chart-ops"), {
        series: [
          { label: "reads/s", lum: 0.9, width: 1.25, dash: [], points: rates.map((r) => [r.t, r.reads]) },
          { label: "writes/s", lum: 0.6, width: 1, dash: [5, 4], points: rates.map((r) => [r.t, r.writes]) },
        ],
        stepped: true,
        zero: true,
        format: (v) => (v === null ? dash : fmtRate(v)),
      });
    if (!drawn) clearChart($("chart-ops"));
    $("chart-ops-empty").hidden = !!drawn;

    const p = app.model.p;
    const cell = (name) => (p ? fmtMs(num(p[name])) : dash);
    $("latency").innerHTML = `<thead><tr><th></th><th class="num">p50</th><th class="num">p95</th><th class="num">p99</th></tr></thead><tbody>
      <tr><td>read</td><td class="num">${cell("read_latency_p50_us")}</td><td class="num">${cell("read_latency_p95_us")}</td><td class="num">${cell("read_latency_p99_us")}</td></tr>
      <tr><td>write</td><td class="num">${cell("write_latency_p50_us")}</td><td class="num">${cell("write_latency_p95_us")}</td><td class="num">${cell("write_latency_p99_us")}</td></tr></tbody>`;
  }

  function svgText(x, y, text, { size = 11, fill = "#a1a1a1", anchor = "start" } = {}) {
    return `<text x="${x}" y="${y}" font-size="${size}" fill="${fill}" text-anchor="${anchor}">${esc(text)}</text>`;
  }

  function renderDurability() {
    const p = app.model.p;
    const errors = p ? num(p.wal_sync_errors_total) : null;
    $("wal-error").innerHTML =
      errors > 0
        ? `<div class="warning" role="alert"><strong>▲ WAL SYNC ERROR</strong>wal_sync_errors_total ${fmtInt(errors)} · the WAL is fail-closed and rejects writes until operator recovery</div>`
        : "";
    if (!p) {
      $("wal-metrics").innerHTML = "<dt>primary</dt><dd>NODE UNAVAILABLE</dd>";
      $("wal-instrument").innerHTML = "";
      return;
    }
    const current = num(p.current_lsn);
    const durable = num(p.durable_lsn);
    const snapshot = num(p.snapshot_lsn);
    $("wal-metrics").innerHTML = [
      ["current_lsn", fmtInt(current)],
      ["durable_lsn", fmtInt(durable)],
      ["in flight", fmtInt(current - durable)],
      ["wal_entries", fmtInt(num(p.wal_entries))],
      ["snapshot_lsn", snapshot ? fmtInt(snapshot) : "0 · none"],
      ["wal_syncs_total", fmtInt(num(p.wal_syncs_total))],
      ["wal_sync_avg", fmtMs(num(p.wal_sync_avg_us))],
      ["wal_sync_max", fmtMs(num(p.wal_sync_max_us))],
      ["wal_sync_errors", fmtInt(errors)],
    ]
      .map(([k, v]) => `<dt>${k}</dt><dd class="${k === "durable_lsn" ? "bright" : ""}">${v}</dd>`)
      .join("");

    const W = Math.max($("wal-instrument").clientWidth, 320);
    const H = 128;
    const base = 66;
    const xs = Math.round(W * 0.05);
    const xd = Math.round(W * 0.74);
    const pending = current - durable;
    const xc = pending > 0 ? Math.round(W * 0.9) : xd;
    const parts = [];
    parts.push(`<line x1="0" y1="${base}" x2="${W}" y2="${base}" stroke="#3a3a3a"/>`);
    parts.push(`<line x1="${xs}" y1="${base}" x2="${xd}" y2="${base}" stroke="#e8e8e8" stroke-width="2"/>`);
    // A break mark: the retained range is drawn compressed, not to scale.
    const xb = Math.round((xs + xd) / 2);
    parts.push(`<rect x="${xb - 7}" y="${base - 6}" width="14" height="12" fill="#090909"/>`);
    parts.push(`<line x1="${xb - 6}" y1="${base + 5}" x2="${xb - 1}" y2="${base - 5}" stroke="#a1a1a1"/><line x1="${xb + 1}" y1="${base + 5}" x2="${xb + 6}" y2="${base - 5}" stroke="#a1a1a1"/>`);
    if (pending > 0) parts.push(`<line x1="${xd}" y1="${base}" x2="${xc}" y2="${base}" stroke="#a1a1a1" stroke-width="2" stroke-dasharray="3 3"/>`);
    const tick = (x, name, lsn, bright) => {
      parts.push(`<line x1="${x}" y1="${base - 14}" x2="${x}" y2="${base + 14}" stroke="${bright ? "#f5f5f5" : "#8a8a8a"}"/>`);
      parts.push(svgText(x, base - 22, name, { fill: "#666666", anchor: "middle" }));
      parts.push(svgText(x, base + 30, lsn, { size: 13, fill: bright ? "#f5f5f5" : "#e8e8e8", anchor: "middle" }));
    };
    tick(xs, "SNAPSHOT", snapshot ? fmtInt(snapshot) : "0", false);
    if (pending > 0) {
      tick(xd, "DURABLE", fmtInt(durable), true);
      tick(xc, "CURRENT", fmtInt(current), false);
      parts.push(svgText(Math.round((xd + xc) / 2), base + 52, `+${fmtInt(pending)} awaiting sync`, { anchor: "middle" }));
    } else {
      tick(xd, "DURABLE = CURRENT", fmtInt(durable), true);
    }
    parts.push(svgText(xb, base + 52, `${fmtInt(num(p.wal_entries))} WAL entries after the snapshot`, { fill: "#666666", anchor: "middle" }));
    parts.push(svgText(W, 10, "positions not to scale", { fill: "#4a4a4a", anchor: "end" }));
    $("wal-instrument").innerHTML = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Log positions: snapshot, durable, current">${parts.join("")}</svg>`;
  }

  function renderStorage() {
    const { p, primary, replicas } = app.model;
    if (!p) {
      $("storage-body").innerHTML = '<p class="placeholder">NODE UNAVAILABLE</p>';
      return;
    }
    if (p.storage_engine !== "lsm") {
      $("storage-desc").textContent = "Engine selected per data directory with --storage.";
      $("storage-body").innerHTML = `<p class="placeholder">LSM DISABLED · storage_engine ${esc(p.storage_engine)} · in-memory map backed by the WAL and snapshots. Nodes started with --storage lsm show their memtable, level-0, and compaction activity here.</p>`;
      return;
    }
    $("storage-desc").textContent = "Writes fill the memtable; a full memtable is flushed to a level-0 table; level-0 tables are compacted into the sorted, non-overlapping level 1.";
    const tables = num(p.lsm_tables);
    const l0 = num(p.lsm_level0_tables);
    const flow = `<div class="lsm-flow">
      <div class="lsm-level"><span class="name">MEMTABLE</span><span>${fmtBytes(num(p.lsm_memtable_bytes))} <span class="meta">in memory</span></span></div>
      <div class="lsm-edge">flush · lsm_flushes_total ${fmtInt(num(p.lsm_flushes_total))}</div>
      <div class="lsm-level"><span class="name">LEVEL 0</span><span>${fmtInt(l0)} tables <span class="meta">overlapping key ranges</span></span></div>
      <div class="lsm-edge">compaction · lsm_compactions_total ${fmtInt(num(p.lsm_compactions_total))}</div>
      <div class="lsm-level"><span class="name">LEVEL 1</span><span>${fmtInt(tables - l0)} tables <span class="meta">sorted, disjoint</span></span></div>
      <div class="lsm-level" style="margin-top:8px"><span class="name meta">ON DISK</span><span>${fmtBytes(num(p.lsm_table_bytes))} <span class="meta">${fmtInt(tables)} tables in all</span></span></div>
    </div>`;
    const nodes = [{ name: primary.name, s: p }].concat(replicas.map((r) => ({ name: r.name, s: r.s })));
    const rows = nodes.map(({ name, s }) => {
      if (!s || s.storage_engine !== "lsm") return `<tr><td>${esc(name)}</td><td colspan="6" class="text">${s ? "memory engine" : "node unavailable"}</td></tr>`;
      return `<tr><td>${esc(name)}</td><td class="num">${fmtBytes(num(s.lsm_memtable_bytes))}</td><td class="num">${fmtInt(num(s.lsm_tables))}</td><td class="num">${fmtInt(num(s.lsm_level0_tables))}</td><td class="num">${fmtBytes(num(s.lsm_table_bytes))}</td><td class="num">${fmtInt(num(s.lsm_flushes_total))}</td><td class="num">${fmtInt(num(s.lsm_compactions_total))}</td></tr>`;
    });
    $("storage-body").innerHTML = `<div class="split">${flow}<div class="table-scroll"><table class="grid"><thead><tr><th>node</th><th class="num">memtable</th><th class="num">tables</th><th class="num">level 0</th><th class="num">on disk</th><th class="num">flushes</th><th class="num">compactions</th></tr></thead><tbody>${rows.join("")}</tbody></table></div></div>`;
  }

  function renderLocks() {
    const p = app.model.p;
    const cell = (name) => (p ? fmtMs(num(p[name])) : dash);
    $("lock-table").innerHTML = `<thead><tr><th></th><th class="num">p50</th><th class="num">p95</th><th class="num">p99</th></tr></thead><tbody>
      <tr><td>read-lock wait</td><td class="num">${cell("read_lock_wait_p50_us")}</td><td class="num">${cell("read_lock_wait_p95_us")}</td><td class="num">${cell("read_lock_wait_p99_us")}</td></tr>
      <tr><td>write-lock hold</td><td class="num">${cell("write_lock_hold_p50_us")}</td><td class="num">${cell("write_lock_hold_p95_us")}</td><td class="num">${cell("write_lock_hold_p99_us")}</td></tr></tbody>`;
    const series = [
      { label: "read wait p99", lum: 0.9, width: 1.25, dash: [], points: app.history.map((h) => [h.t, h.nodes[0] ? num(h.nodes[0].read_lock_wait_p99_us) : null]) },
      { label: "write hold p99", lum: 0.6, width: 1, dash: [5, 4], points: app.history.map((h) => [h.t, h.nodes[0] ? num(h.nodes[0].write_lock_hold_p99_us) : null]) },
    ];
    const drawn = drawChart($("chart-locks"), { series, stepped: true, zero: true, format: (v) => (v === null ? dash : fmtMs(v)) });
    if (!drawn) clearChart($("chart-locks"));
    $("chart-locks-empty").hidden = drawn;
  }

  function renderRecovery() {
    const { primary, p, replicas } = app.model;
    if (p) {
      const snapshot = num(p.snapshot_lsn);
      const replayed = num(p.recovery_records_replayed);
      const took = num(p.recovery_us);
      const W = Math.max($("recovery-diagram").clientWidth, 320);
      const parts = [];
      // The LSM engine recovers from its flushed tables, and its snapshot_lsn
      // moves with every flush, so the current value is not the base the
      // last recovery used. The in-memory engine's snapshot changes only
      // offline, so its current snapshot_lsn is that base.
      const lsm = p.storage_engine === "lsm";
      const baseLabel = lsm ? "FLUSHED TABLES" : "SNAPSHOT";
      const baseValue = lsm ? "memtable rebuilt from WAL" : snapshot ? `LSN ${fmtInt(snapshot)}` : "none · full WAL replay";
      parts.push(svgText(8, 14, baseLabel, { fill: "#666666" }));
      parts.push(svgText(8, 32, baseValue, { size: 13, fill: "#f5f5f5" }));
      parts.push(`<line x1="14" y1="42" x2="14" y2="78" stroke="#8a8a8a"/><line x1="14" y1="78" x2="${W - 140}" y2="78" stroke="#8a8a8a"/>`);
      parts.push(`<rect x="${Math.round(W / 2) - 52}" y="70" width="104" height="16" fill="#090909"/>`);
      parts.push(svgText(Math.round(W / 2), 82, "WAL REPLAY", { anchor: "middle" }));
      parts.push(svgText(Math.round(W / 2), 104, `${fmtInt(replayed)} records`, { size: 13, fill: "#e8e8e8", anchor: "middle" }));
      parts.push(`<line x1="${W - 140}" y1="72" x2="${W - 140}" y2="84" stroke="#f5f5f5"/>`);
      parts.push(svgText(W - 132, 74, "SERVING", { fill: "#666666" }));
      parts.push(svgText(W - 132, 92, `after ${fmtMs(took)}`, { size: 13, fill: "#f5f5f5" }));
      $("recovery-diagram").innerHTML = `<svg width="${W}" height="116" viewBox="0 0 ${W} 116" role="img" aria-label="Recovery: snapshot, then WAL replay">${parts.join("")}</svg>
        <p class="meta mono">${esc(primary.name)}: ${fmtInt(replayed)} records replayed ${lsm ? "after the flushed tables" : "after snapshot LSN " + fmtInt(snapshot)} in ${fmtMs(took)}.</p>`;
    } else {
      $("recovery-diagram").innerHTML = '<p class="placeholder">NODE UNAVAILABLE</p>';
    }
    const nodes = [{ name: primary.name, s: p }].concat(replicas.map((r) => ({ name: r.name, s: r.s })));
    const rows = nodes.map(({ name, s }) =>
      s
        ? `<tr><td>${esc(name)}</td><td class="num">${fmtMs(num(s.recovery_us))}</td><td class="num">${fmtInt(num(s.recovery_records_replayed))}</td><td class="num">${fmtInt(num(s.snapshot_lsn))}</td><td class="num">${fmtInt(num(s.current_lsn))}</td></tr>`
        : `<tr><td>${esc(name)}</td><td colspan="4" class="text">node unavailable</td></tr>`
    );
    $("recovery-table").innerHTML = `<thead><tr><th>node</th><th class="num">recovery</th><th class="num">replayed</th><th class="num">snapshot lsn now</th><th class="num">lsn now</th></tr></thead><tbody>${rows.join("")}</tbody>`;
  }

  // --- key explorer -----------------------------------------------------------
  const explorer = { mode: "get", node: null, encoding: "utf8", view: "utf8", value: null, scanStarts: [] };

  function segment(container, values, current, onSelect) {
    container.innerHTML = values.map(([value, label]) => `<button type="button" data-value="${esc(value)}" aria-selected="${value === current}">${esc(label)}</button>`).join("");
    container.onclick = (event) => {
      const button = event.target.closest("button");
      if (!button) return;
      container.querySelectorAll("button").forEach((b) => b.setAttribute("aria-selected", String(b === button)));
      onSelect(button.dataset.value);
    };
  }

  function inputBytes(text) {
    return explorer.encoding === "hex" ? hexToBytes(text) : encoder.encode(text);
  }

  function setExplorerMode(mode) {
    explorer.mode = mode;
    $("get-form").hidden = mode !== "get";
    $("get-result").hidden = mode !== "get";
    $("scan-form").hidden = mode !== "scan";
    $("scan-result").hidden = mode !== "scan";
  }

  function buildExplorer() {
    const names = app.state.nodes.map((n) => n.name);
    if (!names.includes(explorer.node)) explorer.node = names[0];
    segment($("explorer-node"), names.map((n) => [n, n]), explorer.node, (v) => (explorer.node = v));
    segment($("explorer-encoding"), [["utf8", "UTF-8"], ["hex", "HEX"]], explorer.encoding, (v) => (explorer.encoding = v));
    segment($("explorer-mode"), [["get", "GET / EXISTS"], ["scan", "SCAN"]], explorer.mode, setExplorerMode);
  }

  function renderValue() {
    const box = $("value-view");
    if (!box || !explorer.value) return;
    const text = utf8(explorer.value);
    if (explorer.view === "hex") box.innerHTML = hexDump(explorer.value);
    else if (text === null) box.innerHTML = '<span class="placeholder">not valid UTF-8 · showing hex</span>\n' + hexDump(explorer.value);
    else box.innerHTML = esc(text) || '<span class="placeholder">(empty value)</span>';
  }

  async function runGet(kind) {
    const key = inputBytes($("get-key").value);
    const out = $("get-result");
    if (!key) {
      out.innerHTML = '<p class="placeholder">key is not valid hexadecimal</p>';
      return;
    }
    if (key.length === 0) {
      out.innerHTML = '<p class="placeholder">enter a key</p>';
      return;
    }
    out.innerHTML = '<p class="placeholder">LOADING</p>';
    const query = `node=${encodeURIComponent(explorer.node)}&key=${bytesToHex(key)}`;
    const shown = displayBytes(key).text;
    try {
      if (kind === "exists") {
        const body = await api(`/api/exists?${query}`);
        out.innerHTML = `<div class="result"><h3>Result</h3>${nodeRows([["key", esc(shown)], ["node", esc(body.node)], ["exists", String(body.exists)]])}</div>`;
        return;
      }
      const body = await api(`/api/get?${query}`);
      if (!body.found) {
        out.innerHTML = `<div class="result"><h3>Result</h3>${nodeRows([["key", esc(shown)], ["node", esc(body.node)], ["exists", "false"], ["status", "NOT_FOUND"]])}</div>`;
        return;
      }
      explorer.value = hexToBytes(body.value);
      out.innerHTML = `<div class="result"><h3>Result</h3>${nodeRows([["key", esc(shown)], ["node", esc(body.node)], ["exists", "true"], ["size", explorer.value.length + " B"]])}
        <div class="controls" style="margin:16px 0 0"><span class="label">value</span><span class="seg" id="value-mode"></span></div>
        <div class="value-box" id="value-view"></div></div>`;
      segment($("value-mode"), [["utf8", "UTF-8"], ["hex", "HEX"]], explorer.view, (v) => {
        explorer.view = v;
        renderValue();
      });
      renderValue();
    } catch (error) {
      out.innerHTML = `<p class="placeholder">${esc(errorLine(error))}</p>`;
    }
  }

  async function runScan(page) {
    const out = $("scan-result");
    const start = inputBytes($("scan-start").value);
    const end = inputBytes($("scan-end").value);
    const limit = Number($("scan-limit").value);
    if (!start || !end) {
      out.innerHTML = '<p class="placeholder">bounds are not valid hexadecimal</p>';
      return;
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > 10000) {
      out.innerHTML = '<p class="placeholder">page size must be 1–10000</p>';
      return;
    }
    if (page === 0) explorer.scanStarts = [bytesToHex(start)];
    const from = explorer.scanStarts[page];
    out.innerHTML = '<p class="placeholder">LOADING</p>';
    try {
      const body = await api(`/api/scan?node=${encodeURIComponent(explorer.node)}&start=${from}&end=${end.length ? bytesToHex(end) : ""}&limit=${limit}`);
      if (body.pairs.length === 0) {
        const unbounded = start.length === 0 && end.length === 0 && page === 0;
        out.innerHTML = `<p class="placeholder">${unbounded ? "EMPTY DATABASE · no keys stored on " + esc(body.node) : "NO SCAN RESULTS in the requested range"}</p>`;
        return;
      }
      const offset = page * limit;
      const rows = body.pairs.map(([kHex, vHex], i) => {
        const k = hexToBytes(kHex);
        const v = hexToBytes(vHex);
        const vText = utf8(v);
        const preview = printable(vText)
          ? vText.slice(0, 96) + (vText.length > 96 ? "…" : "")
          : bytesToHex(v.slice(0, 24)).replace(/(..)/g, "$1 ").trim() + (v.length > 24 ? " …" : "");
        return `<tr data-key="${kHex}"><td class="num meta">${offset + i + 1}</td><td>${esc(displayBytes(k).text)}</td><td>${esc(preview || "(empty)")}</td><td class="num">${v.length} B</td></tr>`;
      });
      const last = body.pairs[body.pairs.length - 1][0];
      if (body.more) explorer.scanStarts[page + 1] = last + "00";
      out.innerHTML = `<div class="table-scroll"><table class="grid"><thead><tr><th class="num">#</th><th>key</th><th>value preview</th><th class="num">size</th></tr></thead><tbody>${rows.join("")}</tbody></table></div>
        <div class="pager"><button type="button" id="scan-prev" ${page === 0 ? "disabled" : ""}>PREV</button><button type="button" id="scan-next" ${body.more ? "" : "disabled"}>NEXT</button>
        <span>keys ${offset + 1}–${offset + body.pairs.length}${body.more ? " · more" : " · end of range"} · ${esc(body.node)}</span></div>`;
      $("scan-prev").onclick = () => runScan(page - 1);
      $("scan-next").onclick = () => runScan(page + 1);
      out.querySelectorAll("tbody tr").forEach((tr) => {
        tr.onclick = () => {
          const shown = displayBytes(hexToBytes(tr.dataset.key));
          explorer.encoding = shown.hex ? "hex" : "utf8";
          explorer.mode = "get";
          $("get-key").value = shown.hex ? tr.dataset.key : shown.text;
          buildExplorer();
          setExplorerMode("get");
          runGet("get");
        };
      });
    } catch (error) {
      out.innerHTML = `<p class="placeholder">${esc(errorLine(error))}</p>`;
    }
  }

  // --- write console ----------------------------------------------------------
  const consoleState = { op: "SET", encoding: "utf8", tx: null };

  function consoleBytes(text) {
    return consoleState.encoding === "hex" ? hexToBytes(text) : encoder.encode(text);
  }

  function buildConsole() {
    const body = $("console-body");
    if (!app.state.bridge.writes) {
      body.innerHTML = '<p class="placeholder">WRITES DISABLED · start the bridge with --allow-writes, or run distributedb cluster --dashboard</p>';
      return;
    }
    body.innerHTML = `<div class="controls">
        <span class="seg" id="console-op"></span>
        <span class="label">input</span><span class="seg" id="console-encoding"></span>
      </div>
      <form class="query" id="console-form" autocomplete="off">
        <label class="field"><span class="label">key</span><input class="mono" id="console-key" spellcheck="false"></label>
        <label class="field wide" id="console-value-field"><span class="label">value</span><input class="mono" id="console-value" spellcheck="false"></label>
        <button type="submit" class="primary-action" id="console-submit">EXECUTE</button>
        <button type="button" id="console-begin">BEGIN</button>
      </form>
      <div id="console-tx"></div>
      <div id="console-outcome"></div>`;
    segment($("console-op"), [["SET", "SET"], ["DELETE", "DELETE"]], consoleState.op, (v) => {
      consoleState.op = v;
      $("console-value-field").hidden = v === "DELETE";
    });
    $("console-value-field").hidden = consoleState.op === "DELETE";
    segment($("console-encoding"), [["utf8", "UTF-8"], ["hex", "HEX"]], consoleState.encoding, (v) => (consoleState.encoding = v));
    $("console-form").onsubmit = (event) => {
      event.preventDefault();
      submitWrite();
    };
    $("console-begin").onclick = () => {
      consoleState.tx = [];
      renderTx();
    };
    renderTx();
  }

  function renderTx() {
    const tx = consoleState.tx;
    $("console-submit").textContent = tx ? "QUEUE" : "EXECUTE";
    $("console-begin").disabled = !!tx;
    if (!tx) {
      $("console-tx").innerHTML = "";
      return;
    }
    const items = tx.map((op, i) => `<li><span class="idx">${String(i + 1).padStart(2, "0")}</span><span>${op.op}</span><span>${esc(op.label)}</span></li>`).join("");
    $("console-tx").innerHTML = `<div class="tx-head"><h3 style="margin:0">Transaction</h3>${stateWord("online", "ACTIVE")}<span class="meta">${tx.length} / 64 operations · held in this page; COMMIT sends BEGIN, the writes, and COMMIT on one connection</span></div>
      <ol class="queue">${items || '<li><span class="idx"></span><span class="meta">empty</span><span></span></li>'}</ol>
      <div class="tx-actions"><button type="button" id="tx-rollback">ROLLBACK</button><button type="button" class="primary-action" id="tx-commit" ${tx.length ? "" : "disabled"}>COMMIT ${tx.length} OPS</button></div>`;
    $("tx-rollback").onclick = () => {
      consoleState.tx = null;
      outcome(false, "ROLLED BACK", `${tx.length} queued operations discarded; nothing was sent`);
      renderTx();
    };
    $("tx-commit").onclick = commitTx;
  }

  function outcome(failed, title, detail) {
    $("console-outcome").innerHTML = `<div class="outcome${failed ? " failed" : ""}"><strong>${esc(title)}</strong> ${esc(detail)}</div>`;
  }

  function describe(result) {
    const ms = (result.round_trip_us / 1000).toFixed(2) + " ms round trip";
    if (result.status === "OK") return [false, `operations ${result.operations} · status OK · durability acknowledged · ${ms}`];
    if (result.status === "OK_VOLATILE") return [false, `operations ${result.operations} · status OK_VOLATILE · applied without sync (os mode) · ${ms}`];
    const where = result.failed_at ? ` at ${result.failed_at}` : "";
    const unknown = result.status === "UNAVAILABLE" ? " · outcome unknown: the write may be durable on the primary" : "";
    return [true, `status ${result.status}${where}${unknown} · ${ms}`];
  }

  async function submitWrite() {
    const key = consoleBytes($("console-key").value);
    const value = consoleState.op === "SET" ? consoleBytes($("console-value").value) : new Uint8Array();
    if (!key || !value) {
      outcome(true, "WRITE FAILURE", "input is not valid hexadecimal");
      return;
    }
    if (key.length === 0) {
      outcome(true, "WRITE FAILURE", "enter a key");
      return;
    }
    const label = consoleState.op === "SET" ? `${displayBytes(key).text} ${displayBytes(value).text}` : displayBytes(key).text;
    if (consoleState.tx) {
      if (consoleState.tx.length >= 64) {
        outcome(true, "WRITE FAILURE", "a transaction holds at most 64 operations (one WAL group)");
        return;
      }
      consoleState.tx.push({ op: consoleState.op, key: bytesToHex(key), value: bytesToHex(value), label });
      renderTx();
      return;
    }
    try {
      const path = consoleState.op === "SET" ? `/api/set?key=${bytesToHex(key)}&value=${bytesToHex(value)}` : `/api/delete?key=${bytesToHex(key)}`;
      const result = await post(path);
      const [failed, detail] = describe(result);
      outcome(failed, failed ? "WRITE FAILURE" : `${consoleState.op} ${label}`, detail);
    } catch (error) {
      outcome(true, "WRITE FAILURE", errorLine(error));
    }
  }

  async function commitTx() {
    const tx = consoleState.tx;
    const lines = tx.map((op) => (op.op === "SET" ? `SET ${op.key} ${op.value || "-"}` : `DELETE ${op.key}`)).join("\n");
    $("tx-commit").disabled = true;
    try {
      const result = await post("/api/transaction", lines);
      const [failed, detail] = describe(result);
      consoleState.tx = null;
      renderTx();
      outcome(failed, failed ? "WRITE FAILURE" : "COMMITTED", detail);
    } catch (error) {
      $("tx-commit").disabled = false;
      outcome(true, "WRITE FAILURE", errorLine(error) + " · transaction still queued");
    }
  }

  // --- fault-tolerance lab ----------------------------------------------------
  const lab = { running: false, steps: null, target: null };
  const STEP_NAMES = ["SEED DATABASE", "STOP REPLICA", "CONTINUE WRITES", "RESTART REPLICA", "OBSERVE CATCH-UP"];

  function buildLab() {
    const body = $("lab-body");
    const bridge = app.state.bridge;
    const names = app.state.nodes.map((n) => n.name);
    if (!bridge.control || !bridge.writes) {
      body.innerHTML = '<p class="placeholder">REQUIRES THE CLUSTER LAUNCHER · run distributedb cluster --replicas 2 --dashboard so this page can stop and restart replicas</p>';
      return;
    }
    if (names.length < 2) {
      body.innerHTML = '<p class="placeholder">REQUIRES AT LEAST ONE REPLICA</p>';
      return;
    }
    lab.target = names[names.length - 1];
    lab.steps = STEP_NAMES.map((name) => ({ name: name.replace("REPLICA", lab.target.toUpperCase()), state: "pending", detail: "" }));
    body.innerHTML = `<p class="meta mono" style="margin-bottom:12px">target ${esc(lab.target)} · 200 seed writes · 300 writes while stopped · keys lab:NNNNNNNN</p>
      <ol class="steps" id="lab-steps"></ol>
      <button type="button" class="primary-action" id="lab-run">RUN EXPERIMENT</button> <span class="meta mono" id="lab-status"></span>`;
    $("lab-run").onclick = runLab;
    renderLab();
  }

  function renderLab() {
    if (!lab.steps || !$("lab-steps")) return;
    $("lab-steps").innerHTML = lab.steps
      .map((step, i) => `<li class="${step.state}"><span>${String(i + 1).padStart(2, "0")}</span><span>${esc(step.name)}</span><span class="step-state">${step.state.toUpperCase()}</span><span class="detail">${esc(step.detail)}</span></li>`)
      .join("");
  }

  function until(predicate, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    return new Promise((resolve, reject) => {
      (function check() {
        let value = null;
        try {
          value = app.model && !app.bridgeDown && predicate(app.model);
        } catch (_) {
          value = null;
        }
        if (value) return resolve(value);
        if (Date.now() > deadline) return reject(new Error("timed out after " + timeoutMs / 1000 + " s"));
        setTimeout(check, 200);
      })();
    });
  }

  async function labWrites(from, count) {
    let next = from;
    const end = from + count;
    async function worker() {
      while (next < end) {
        const n = next++;
        const key = bytesToHex(encoder.encode("lab:" + String(n).padStart(8, "0")));
        const value = bytesToHex(encoder.encode("v" + n + "@" + Date.now()));
        const result = await post(`/api/set?key=${key}&value=${value}`);
        if (result.status !== "OK" && result.status !== "OK_VOLATILE") throw new Error("write returned " + result.status);
      }
    }
    await Promise.all([worker(), worker(), worker(), worker()]);
  }

  const targetOf = (m) => m.replicas.find((r) => r.name === lab.target);

  async function runLab() {
    if (lab.running) return;
    lab.running = true;
    $("lab-run").disabled = true;
    $("lab-status").textContent = "running";
    lab.steps.forEach((s) => {
      s.state = "pending";
      s.detail = "";
    });
    renderLab();
    const step = async (i, work) => {
      lab.steps[i].state = "running";
      renderLab();
      lab.steps[i].detail = await work();
      lab.steps[i].state = "complete";
      renderLab();
    };
    const base = (Math.floor(Date.now() / 1000) % 100000) * 1000;
    let restartedAt = 0;
    try {
      // Start from a replica that is streaming.
      const initial = targetOf(app.model);
      if (initial && !initial.up) await post("/api/node/start?name=" + encodeURIComponent(lab.target));
      await until((m) => ["synced", "lagging"].includes(targetOf(m).state), 30000);
      await step(0, async () => {
        await labWrites(base, 200);
        const m = await until((m) => m.p && m, 5000);
        return `200 writes · durable_lsn ${fmtInt(num(m.p.durable_lsn))}`;
      });
      await step(1, async () => {
        await post("/api/node/stop?name=" + encodeURIComponent(lab.target));
        // Wait until the primary itself has seen the session close, not just
        // until this page has seen the process exit.
        const m = await until((m) => targetOf(m).state === "disconnected" && m.connected < m.replicas.length && m, 15000);
        return `${lab.target} stopped · last acknowledged lsn ${fmtInt(targetOf(m).applied)} · replicas connected ${fmtInt(m.connected)} / ${m.replicas.length}`;
      });
      await step(2, async () => {
        await labWrites(base + 200, 300);
        const m = await until((m) => m.p && m, 5000);
        const r = targetOf(m);
        return `300 writes · durable_lsn ${fmtInt(num(m.p.durable_lsn))} · ${lab.target} behind by ${fmtInt(num(m.p.durable_lsn) - r.applied)} entries`;
      });
      let missedThrough = 0;
      await step(3, async () => {
        missedThrough = num(app.model.p.durable_lsn);
        await post("/api/node/start?name=" + encodeURIComponent(lab.target));
        restartedAt = Date.now();
        const m = await until((m) => targetOf(m).up && targetOf(m).s && m, 30000);
        return `${lab.target} process running · local lsn ${fmtInt(num(targetOf(m).s.current_lsn))}`;
      });
      // Caught up: the replica has applied every entry that was durable on
      // the primary when it restarted, i.e. everything it missed.
      await step(4, async () => {
        const m = await until((m) => {
          const r = targetOf(m);
          return ["synced", "lagging"].includes(r.state) && r.applied >= missedThrough && m;
        }, 60000);
        const r = targetOf(m);
        return `applied ${fmtInt(r.applied)} ≥ ${fmtInt(missedThrough)} (durable at restart) ${((Date.now() - restartedAt) / 1000).toFixed(1)} s after restart (±1 s poll) · lag now ${fmtInt(r.lag)} · ${r.state.toUpperCase()}`;
      });
      $("lab-status").textContent = "complete";
    } catch (error) {
      const failed = lab.steps.find((s) => s.state === "running");
      if (failed) {
        failed.state = "failed";
        failed.detail = error instanceof BridgeError ? errorLine(error) : error.message;
      }
      renderLab();
      $("lab-status").textContent = failed ? "stopped" : "stopped · " + (error.message || error);
    } finally {
      lab.running = false;
      $("lab-run").disabled = false;
    }
  }

  // --- main loop --------------------------------------------------------------
  function renderAll() {
    renderHeader();
    renderBanner();
    if (!app.model) return;
    renderIdentity();
    renderTree();
    renderSummary();
    renderReplication();
    renderTraffic();
    renderDurability();
    renderStorage();
    renderLocks();
    renderRecovery();
    const key = `${app.state.nodes.map((n) => n.name).join(",")}|${app.state.bridge.writes}|${app.state.bridge.control}`;
    if (app.builtFor !== key) {
      app.builtFor = key;
      buildExplorer();
      buildConsole();
      if (!lab.running) buildLab();
    }
    if (app.topology) {
      const p = app.model.p;
      app.topology.update({
        hasData: true,
        primaryUp: !!p,
        currentLsn: p ? num(p.current_lsn) : 0,
        durableLsn: p ? num(p.durable_lsn) : 0,
        replicas: app.model.replicas.map((r) => ({ name: r.name, state: r.state === "online" ? "unknown" : r.state })),
      });
    }
  }

  function validState(state) {
    return (
      state &&
      Array.isArray(state.nodes) &&
      state.nodes.length > 0 &&
      state.bridge &&
      state.nodes.every((n) => typeof n.name === "string" && n.stats && typeof n.stats === "object")
    );
  }

  async function poll() {
    try {
      const state = await api("/api/state");
      if (!validState(state)) throw new BridgeError("malformed", "malformed response from /api/state");
      app.state = state;
      app.lastOk = Date.now();
      app.bridgeDown = false;
      app.malformed = false;
      app.model = derive(state);
      ingest(state);
      renderAll();
    } catch (error) {
      app.malformed = error.kind === "malformed";
      app.bridgeDown = !app.malformed;
      renderHeader();
      renderBanner();
      // Without the bridge, the model shows no state rather than the last one.
      if (app.topology) {
        const replicas = app.model ? app.model.replicas.map((r) => ({ name: r.name, state: "unknown" })) : [];
        app.topology.update({ hasData: false, primaryUp: false, currentLsn: 0, durableLsn: 0, replicas });
      }
    }
    setTimeout(poll, POLL_MS);
  }

  async function start() {
    app.topology = window.Topology ? window.Topology.mount($("topology"), $("topology-labels")) : null;
    ["chart-repl", "chart-ops", "chart-locks"].forEach((id) => attachHover($(id)));
    $("get-form").onsubmit = (event) => {
      event.preventDefault();
      runGet("get");
    };
    $("exists-button").onclick = () => runGet("exists");
    $("scan-form").onsubmit = (event) => {
      event.preventDefault();
      runScan(0);
    };
    window.addEventListener("resize", () => {
      if (app.model) renderAll();
    });
    try {
      const history = await api("/api/history");
      if (history && Array.isArray(history.samples)) {
        app.history = history.samples.map((s) => ({ t: s.t, nodes: s.nodes })).slice(-HISTORY_MAX);
      }
    } catch (_) {
      // The first state poll reports the bridge's condition.
    }
    poll();
  }

  start();
})();
