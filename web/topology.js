/* Write-path model for the opening region, drawn with raw WebGL.
 *
 * Three regions: the primary (a sequencer path feeding an ordered log line,
 * inside a sparse constellation) and one constellation per replica. A write
 * appears as a white impulse: sequencer, along the log, then out to each
 * replica, which brightens briefly when the impulse arrives. Impulses are
 * spawned only when the primary's current_lsn advances, so an idle cluster
 * shows no traffic. Lagging replicas receive fewer impulses and dim;
 * disconnected replicas get a dashed link and none. Monochrome throughout. */
(function () {
  "use strict";

  const VERTEX = `
    attribute vec3 a_pos;
    attribute vec2 a_la;
    attribute float a_size;
    uniform mat4 u_mvp;
    uniform float u_dpr;
    varying vec2 v_la;
    void main() {
      gl_Position = u_mvp * vec4(a_pos, 1.0);
      gl_PointSize = a_size * u_dpr;
      v_la = a_la;
    }`;
  const FRAGMENT = `
    precision mediump float;
    varying vec2 v_la;
    uniform float u_points;
    void main() {
      float a = v_la.y;
      if (u_points > 0.5) {
        float d = length(gl_PointCoord - 0.5);
        if (d > 0.5) discard;
        a *= 1.0 - smoothstep(0.3, 0.5, d);
      }
      gl_FragColor = vec4(vec3(v_la.x) * a, a);
    }`;

  // --- small math ---------------------------------------------------------
  function mulberry32(seed) {
    return function () {
      seed |= 0;
      seed = (seed + 0x6d2b79f5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const add = (a, b) => [a[0] + b[0], a[1] + b[1], a[2] + b[2]];
  const sub = (a, b) => [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
  const scale = (a, s) => [a[0] * s, a[1] * s, a[2] * s];
  const lerp = (a, b, t) => add(a, scale(sub(b, a), t));
  const dist = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
  function normalize(a) {
    const l = Math.hypot(a[0], a[1], a[2]) || 1;
    return [a[0] / l, a[1] / l, a[2] / l];
  }
  function cross(a, b) {
    return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
  }
  function perspective(fovy, aspect, near, far) {
    const f = 1 / Math.tan(fovy / 2);
    const nf = 1 / (near - far);
    return [f / aspect, 0, 0, 0, 0, f, 0, 0, 0, 0, (far + near) * nf, -1, 0, 0, 2 * far * near * nf, 0];
  }
  function lookAt(eye, center, up) {
    const z = normalize(sub(eye, center));
    const x = normalize(cross(up, z));
    const y = cross(z, x);
    return [
      x[0], y[0], z[0], 0,
      x[1], y[1], z[1], 0,
      x[2], y[2], z[2], 0,
      -(x[0] * eye[0] + x[1] * eye[1] + x[2] * eye[2]),
      -(y[0] * eye[0] + y[1] * eye[1] + y[2] * eye[2]),
      -(z[0] * eye[0] + z[1] * eye[1] + z[2] * eye[2]),
      1,
    ];
  }
  function multiply(a, b) {
    const out = new Array(16);
    for (let c = 0; c < 4; c++) {
      for (let r = 0; r < 4; r++) {
        let s = 0;
        for (let k = 0; k < 4; k++) s += a[k * 4 + r] * b[c * 4 + k];
        out[c * 4 + r] = s;
      }
    }
    return out;
  }
  function project(m, p) {
    const x = m[0] * p[0] + m[4] * p[1] + m[8] * p[2] + m[12];
    const y = m[1] * p[0] + m[5] * p[1] + m[9] * p[2] + m[13];
    const w = m[3] * p[0] + m[7] * p[1] + m[11] * p[2] + m[15];
    return [x / w, y / w];
  }

  // --- scene ----------------------------------------------------------------
  function constellation(rand, center, radii, count) {
    const points = [];
    for (let i = 0; i < count; i++) {
      // Points on a few loose shells read as structure rather than noise.
      const shell = 0.45 + 0.55 * ((i % 3) / 2) * (0.75 + 0.25 * rand());
      const u = rand() * 2 - 1;
      const theta = rand() * Math.PI * 2;
      const r = Math.sqrt(1 - u * u);
      points.push(add(center, [radii[0] * shell * r * Math.cos(theta), radii[1] * shell * u, radii[2] * shell * r * Math.sin(theta)]));
    }
    return points;
  }

  function nearestEdges(points, offset, perPoint) {
    const edges = new Set();
    points.forEach((p, i) => {
      points
        .map((q, j) => [j, dist(p, q)])
        .filter(([j]) => j !== i)
        .sort((a, b) => a[1] - b[1])
        .slice(0, perPoint)
        .forEach(([j]) => {
          edges.add(Math.min(i, j) + offset + ":" + (Math.max(i, j) + offset));
        });
    });
    return [...edges].map((key) => key.split(":").map(Number));
  }

  function buildScene(replicaCount) {
    const rand = mulberry32(20261006);
    const primary = [-1.05, 0, 0];
    const vertices = []; // {p, kind, group, phase, walIndex}
    const edges = []; // [a, b, kind, group]
    const push = (p, kind, group) => vertices.push({ p, kind, group, phase: rand() * Math.PI * 2, walIndex: -1 }) - 1;

    const wal = [];
    const walCount = 24;
    for (let i = 0; i < walCount; i++) {
      const t = i / (walCount - 1);
      const index = push(add(primary, [-0.55 + 1.1 * t, 0.07 * Math.sin(Math.PI * t) - 0.02, 0.14 * Math.cos(Math.PI * t)]), "wal", -1);
      vertices[index].walIndex = i;
      wal.push(index);
    }
    const seq = [];
    const seqStart = add(primary, [-1.02, 0.52, 0.18]);
    for (let i = 0; i < 6; i++) {
      const t = i / 6;
      const p = lerp(seqStart, vertices[wal[0]].p, t);
      seq.push(push(add(p, [0, 0.05 * Math.sin(Math.PI * t), 0]), "seq", -1));
    }
    seq.push(wal[0]);
    for (let i = 1; i < seq.length; i++) edges.push([seq[i - 1], seq[i], "path", -1]);
    for (let i = 1; i < wal.length; i++) edges.push([wal[i - 1], wal[i], "log", -1]);

    const cloudStart = vertices.length;
    const cloud = constellation(rand, primary, [0.68, 0.52, 0.46], 50);
    cloud.forEach((p) => push(p, "cloud", -1));
    nearestEdges(cloud, cloudStart, 2).forEach(([a, b]) => edges.push([a, b, "local", -1]));
    for (let i = 0; i < 9; i++) {
      const c = cloudStart + Math.floor(rand() * cloud.length);
      let best = wal[0];
      wal.forEach((w) => {
        if (dist(vertices[w].p, vertices[c].p) < dist(vertices[best].p, vertices[c].p)) best = w;
      });
      edges.push([c, best, "local", -1]);
    }

    const replicas = [];
    for (let k = 0; k < replicaCount; k++) {
      const center = [1.2, ((replicaCount - 1) / 2 - k) * (replicaCount > 2 ? 0.8 : 1.0), k % 2 ? -0.2 : 0.2];
      const log = [];
      for (let i = 0; i < 9; i++) {
        const t = i / 8;
        log.push(push(add(center, [-0.27 + 0.54 * t, 0.03 * Math.sin(Math.PI * t), 0.06 * Math.cos(Math.PI * t)]), "rlog", k));
      }
      for (let i = 1; i < log.length; i++) edges.push([log[i - 1], log[i], "log", k]);
      const start = vertices.length;
      const points = constellation(rand, center, [0.36, 0.3, 0.28], 22);
      points.forEach((p) => push(p, "cloud", k));
      nearestEdges(points, start, 2).forEach(([a, b]) => edges.push([a, b, "local", k]));
      for (let i = 0; i < 4; i++) {
        edges.push([start + Math.floor(rand() * points.length), log[Math.floor(rand() * log.length)], "local", k]);
      }
      // One faint structural line between the constellations, besides the
      // replication link itself.
      let near = cloudStart;
      cloud.forEach((p, i) => {
        if (dist(p, center) < dist(vertices[near].p, center)) near = cloudStart + i;
      });
      edges.push([near, start, "bridge", k]);
      replicas.push({ center, log, link: [wal[wal.length - 1], log[0]] });
    }
    return { vertices, edges, wal, seq, replicas, primary };
  }

  function polyline(scene, indices) {
    const points = indices.map((i) => scene.vertices[i].p);
    const lengths = [0];
    for (let i = 1; i < points.length; i++) lengths.push(lengths[i - 1] + dist(points[i - 1], points[i]));
    return { points, lengths, total: lengths[lengths.length - 1] };
  }

  function pointAt(path, t) {
    const target = t * path.total;
    for (let i = 1; i < path.points.length; i++) {
      if (path.lengths[i] >= target) {
        const span = path.lengths[i] - path.lengths[i - 1] || 1;
        return lerp(path.points[i - 1], path.points[i], (target - path.lengths[i - 1]) / span);
      }
    }
    return path.points[path.points.length - 1];
  }

  // --- renderer ---------------------------------------------------------------
  const STATE_LUMINANCE = { synced: 1, lagging: 0.7, disconnected: 0.32, unknown: 0.45 };
  const LINK_ALPHA = { synced: 0.17, lagging: 0.08, disconnected: 0.08, unknown: 0.06 };

  function mount(canvas, labels) {
    const gl = canvas.getContext("webgl", { antialias: true, premultipliedAlpha: true, alpha: true });
    if (!gl) {
      const note = document.createElement("div");
      note.className = "model-fallback";
      note.textContent = "WebGL unavailable. Topology is listed under Cluster.";
      canvas.parentNode.appendChild(note);
      return { update() {} };
    }
    const program = gl.createProgram();
    [[gl.VERTEX_SHADER, VERTEX], [gl.FRAGMENT_SHADER, FRAGMENT]].forEach(([type, source]) => {
      const shader = gl.createShader(type);
      gl.shaderSource(shader, source);
      gl.compileShader(shader);
      gl.attachShader(program, shader);
    });
    gl.linkProgram(program);
    gl.useProgram(program);
    const loc = {
      pos: gl.getAttribLocation(program, "a_pos"),
      la: gl.getAttribLocation(program, "a_la"),
      size: gl.getAttribLocation(program, "a_size"),
      mvp: gl.getUniformLocation(program, "u_mvp"),
      dpr: gl.getUniformLocation(program, "u_dpr"),
      points: gl.getUniformLocation(program, "u_points"),
    };
    const buffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    const stride = 6 * 4;
    gl.enableVertexAttribArray(loc.pos);
    gl.vertexAttribPointer(loc.pos, 3, gl.FLOAT, false, stride, 0);
    gl.enableVertexAttribArray(loc.la);
    gl.vertexAttribPointer(loc.la, 2, gl.FLOAT, false, stride, 12);
    gl.enableVertexAttribArray(loc.size);
    gl.vertexAttribPointer(loc.size, 1, gl.FLOAT, false, stride, 20);
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    let scene = null;
    let writePath = null;
    let linkPaths = [];
    let model = { hasData: false, primaryUp: false, currentLsn: 0, durableLsn: 0, replicas: [] };
    let lastLsn = null;
    let head = 0; // log index of the newest write
    const signals = [];
    const pending = []; // [time, fn]
    let flashes = [];
    let catching = [];
    let previousStates = [];
    let pointer = [0, 0];
    let visible = true;
    let labelNodes = [];

    function makeLabel(text) {
      const node = document.createElement("div");
      node.className = "model-label";
      const name = document.createElement("span");
      name.textContent = text;
      const sub = document.createElement("span");
      sub.className = "sub";
      node.append(name, sub);
      labels.appendChild(node);
      return node;
    }

    function rebuild(count) {
      scene = buildScene(count);
      writePath = polyline(scene, scene.seq.concat(scene.wal.slice(1)));
      linkPaths = scene.replicas.map((r) => polyline(scene, [r.link[0]].concat(r.log)));
      flashes = new Array(count).fill(0);
      catching = new Array(count).fill(false);
      previousStates = new Array(count).fill("unknown");
      signals.length = 0;
      labels.textContent = "";
      labelNodes = [makeLabel("PRIMARY")];
      for (let k = 0; k < count; k++) labelNodes.push(makeLabel("REPLICA " + String(k + 1).padStart(2, "0")));
    }

    function replicaState(k) {
      return (model.replicas[k] && model.replicas[k].state) || "unknown";
    }

    function spawnWrite(now) {
      signals.push({ path: writePath, start: now, duration: 1500, kind: "write" });
    }

    function spawnReplication(now, k) {
      if (!linkPaths[k]) return;
      const duration = catching[k] ? 650 : 1300;
      signals.push({ path: linkPaths[k], start: now, duration, kind: "repl", replica: k });
    }

    function update(next) {
      if (next.replicas.length !== scene.replicas.length) rebuild(next.replicas.length);
      model = next;
      const now = performance.now();
      next.replicas.forEach((replica, k) => {
        if (previousStates[k] === "disconnected" && replica.state !== "disconnected") catching[k] = true;
        if (replica.state === "synced" || replica.state === "disconnected") catching[k] = false;
        previousStates[k] = replica.state;
      });
      if (!next.hasData || !next.primaryUp) {
        lastLsn = null;
        return;
      }
      if (lastLsn !== null && next.currentLsn > lastLsn && !reduced) {
        // A few impulses per poll at most: the model shows that writes
        // flow, not how many.
        const impulses = Math.min(next.currentLsn - lastLsn, 4);
        for (let i = 0; i < impulses; i++) {
          pending.push([now + Math.random() * 900, () => spawnWrite(performance.now())]);
        }
      }
      // A reconnected replica catching up receives impulses even without
      // new writes, until it reports caught up.
      catching.forEach((on, k) => {
        if (on && !reduced) {
          for (let i = 0; i < 3; i++) pending.push([now + i * 300, () => spawnReplication(performance.now(), k)]);
        }
      });
      lastLsn = next.currentLsn;
    }

    function resize() {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.round(canvas.clientWidth * dpr);
      const height = Math.round(canvas.clientHeight * dpr);
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      return dpr;
    }

    function frame(time) {
      requestAnimationFrame(frame);
      if (!visible || canvas.clientWidth === 0) return;
      const dpr = resize();
      const now = performance.now();
      for (let i = pending.length - 1; i >= 0; i--) {
        if (pending[i][0] <= now) {
          const run = pending[i][1];
          pending.splice(i, 1);
          run();
        }
      }

      const aspect = canvas.width / Math.max(canvas.height, 1);
      const seconds = reduced ? 0 : time / 1000;
      const yaw = 0.2 * Math.sin((seconds / 90) * Math.PI * 2) + pointer[0] * 0.04;
      const pitch = 0.16 + 0.03 * Math.sin((seconds / 120) * Math.PI * 2) + pointer[1] * 0.025;
      const distance = 4.3 * Math.max(1, 1.5 / aspect);
      const eye = [distance * Math.sin(yaw) * Math.cos(pitch), distance * Math.sin(pitch), distance * Math.cos(yaw) * Math.cos(pitch)];
      const mvp = multiply(perspective(0.66, aspect, 0.1, 50), lookAt(eye, [0.08, 0, 0], [0, 1, 0]));

      const present = model.hasData && model.primaryUp ? 1 : 0.45;
      const walCount = scene.wal.length;
      const lines = [];
      const points = [];

      function vertexLuminance(v) {
        let lum = v.kind === "wal" || v.kind === "seq" ? 0.72 : 0.62;
        if (v.walIndex >= 0 && model.hasData) {
          const behind = (head - v.walIndex + walCount) % walCount;
          if (behind < 6) lum += 0.28 * (1 - behind / 6);
          // An entry past durable_lsn: written, awaiting the group's sync.
          if (model.currentLsn > model.durableLsn && v.walIndex === (head + 1) % walCount) lum = 0.4;
        }
        if (v.group >= 0) {
          lum *= STATE_LUMINANCE[replicaState(v.group)];
          lum += 0.3 * flashes[v.group];
        }
        const breathing = reduced ? 1 : 0.93 + 0.07 * Math.sin(seconds * 0.7 + v.phase);
        return Math.min(lum * breathing * present, 1);
      }

      scene.edges.forEach(([a, b, kind, group]) => {
        let alpha = kind === "log" ? 0.2 : kind === "path" ? 0.16 : kind === "bridge" ? 0.05 : 0.07;
        if (group >= 0) alpha *= STATE_LUMINANCE[replicaState(group)];
        alpha *= present;
        lines.push(scene.vertices[a].p, [0.9, alpha], scene.vertices[b].p, [0.9, alpha]);
      });
      scene.replicas.forEach((replica, k) => {
        const state = replicaState(k);
        const from = scene.vertices[replica.link[0]].p;
        const to = scene.vertices[replica.link[1]].p;
        const alpha = LINK_ALPHA[state] * present;
        if (state === "disconnected") {
          const pieces = 16;
          for (let i = 0; i < pieces; i += 2) {
            lines.push(lerp(from, to, i / pieces), [0.85, alpha], lerp(from, to, (i + 1) / pieces), [0.85, alpha]);
          }
        } else {
          lines.push(from, [0.95, alpha], to, [0.95, alpha]);
        }
      });

      scene.vertices.forEach((v) => {
        const lum = vertexLuminance(v);
        const size = v.kind === "wal" ? 3.2 : v.kind === "seq" || v.kind === "rlog" ? 2.6 : 2.2;
        points.push([v.p, [0.55 + 0.45 * lum, 0.35 + 0.6 * lum], size]);
      });

      for (let i = signals.length - 1; i >= 0; i--) {
        const signal = signals[i];
        if (signal.kind === "repl" && replicaState(signal.replica) === "disconnected") {
          signals.splice(i, 1);
          continue;
        }
        const t = (now - signal.start) / signal.duration;
        if (t >= 1) {
          signals.splice(i, 1);
          if (signal.kind === "write") {
            head = (head + 1) % walCount;
            scene.replicas.forEach((_, k) => {
              const state = replicaState(k);
              if (state === "disconnected") return;
              if (state === "lagging" && Math.random() > 0.35) return;
              spawnReplication(now, k);
            });
          } else {
            flashes[signal.replica] = 1;
          }
          continue;
        }
        const eased = t * t * (3 - 2 * t);
        points.push([pointAt(signal.path, eased), [1, 0.95], 3.6]);
        points.push([pointAt(signal.path, Math.max(eased - 0.03, 0)), [1, 0.25], 2.6]);
      }
      flashes = flashes.map((f) => Math.max(0, f - 0.012));

      const count = lines.length / 2 + points.length;
      const data = new Float32Array(count * 6);
      let o = 0;
      for (let i = 0; i < lines.length; i += 2) {
        data.set(lines[i], o);
        data.set(lines[i + 1], o + 3);
        data[o + 5] = 1;
        o += 6;
      }
      points.forEach(([p, la, size]) => {
        data.set(p, o);
        data.set(la, o + 3);
        data[o + 5] = size;
        o += 6;
      });

      gl.viewport(0, 0, canvas.width, canvas.height);
      gl.clearColor(0, 0, 0, 0);
      gl.clear(gl.COLOR_BUFFER_BIT);
      gl.uniformMatrix4fv(loc.mvp, false, mvp);
      gl.uniform1f(loc.dpr, dpr);
      gl.bufferData(gl.ARRAY_BUFFER, data, gl.DYNAMIC_DRAW);
      gl.uniform1f(loc.points, 0);
      gl.drawArrays(gl.LINES, 0, lines.length / 2);
      gl.uniform1f(loc.points, 1);
      gl.drawArrays(gl.POINTS, lines.length / 2, points.length);

      // Region labels follow the projected centers.
      const place = (node, p, note, dim) => {
        const s = project(mvp, p);
        node.style.left = ((s[0] + 1) / 2) * canvas.clientWidth + "px";
        node.style.top = ((1 - s[1]) / 2) * canvas.clientHeight + "px";
        node.style.opacity = dim ? 0.5 : 1;
        node.lastChild.textContent = note;
      };
      const primaryNote = model.hasData ? (model.primaryUp ? "" : "UNAVAILABLE") : "NO DATA";
      place(labelNodes[0], add(scene.primary, [0, -0.72, 0]), primaryNote, !model.primaryUp);
      scene.replicas.forEach((replica, k) => {
        const state = replicaState(k);
        place(labelNodes[k + 1], add(replica.center, [0, -0.46, 0]), state === "unknown" ? "" : state.toUpperCase(), state === "disconnected");
      });
    }

    rebuild(0);
    canvas.addEventListener("pointermove", (event) => {
      const rect = canvas.getBoundingClientRect();
      pointer = [((event.clientX - rect.left) / rect.width) * 2 - 1, ((event.clientY - rect.top) / rect.height) * 2 - 1];
    });
    canvas.addEventListener("pointerleave", () => {
      pointer = [0, 0];
    });
    if ("IntersectionObserver" in window) {
      new IntersectionObserver((entries) => {
        visible = entries[0].isIntersecting;
      }).observe(canvas);
    }
    requestAnimationFrame(frame);
    return { update };
  }

  window.Topology = { mount };
})();
