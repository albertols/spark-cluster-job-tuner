// Tuner API client.
//
// Phase 2: real fetch + long-poll. Resolves to no-op behaviour when
// `serve.sh` is running in static-only mode (no /api/health endpoint),
// so the wizard can detect API availability with `await TunerApi.health()`
// and gracefully fall back to the Phase-1 IntelliJ panel.

const TunerApi = (() => {

  // ── Low-level fetch helper ────────────────────────────────────────────────

  async function jsonFetch(url, opts = {}) {
    const r = await fetch(url, opts);
    const text = await r.text();
    let body = null;
    if (text) {
      try { body = JSON.parse(text); } catch (_) { body = text; }
    }
    if (!r.ok) {
      const err = new Error(`${opts.method || "GET"} ${url} → ${r.status}`);
      err.status = r.status;
      err.body = body;
      throw err;
    }
    return body;
  }

  // ── Health probe (~200 ms timeout) ────────────────────────────────────────

  async function health() {
    try {
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), 600);
      const r = await fetch("api/health", { cache: "no-store", signal: ctl.signal });
      clearTimeout(timer);
      if (!r.ok) return false;
      const j = await r.json();
      return !!(j && j.ok);
    } catch (_) {
      return false;
    }
  }

  // ── Config ────────────────────────────────────────────────────────────────

  async function getConfig() {
    return jsonFetch("api/config");
  }

  async function persistConfig(patch) {
    return jsonFetch("api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
  }

  // ── Inputs ────────────────────────────────────────────────────────────────

  async function listInputs() {
    return jsonFetch("api/inputs");
  }

  async function mkdirInput(date) {
    return jsonFetch(`api/inputs/${encodeURIComponent(date)}`, { method: "POST" });
  }

  async function uploadCsv(date, name, file) {
    const url = `api/inputs/${encodeURIComponent(date)}/csv?name=${encodeURIComponent(name)}`;
    const r = await fetch(url, {
      method: "PUT",
      headers: { "Content-Type": "text/csv" },
      body: file,
    });
    const text = await r.text().catch(() => "");
    let body = null;
    if (text) { try { body = JSON.parse(text); } catch (_) { body = text; } }
    if (!r.ok) {
      const err = new Error(`PUT ${url} → ${r.status}`);
      err.status = r.status;
      err.body = body;
      throw err;
    }
    return body;
  }

  // ── Runs ──────────────────────────────────────────────────────────────────

  async function startSingle(params) {
    return jsonFetch("api/runs/single", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
  }

  async function startAuto(params) {
    return jsonFetch("api/runs/auto", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
  }

  async function getRunStatus(runId) {
    return jsonFetch(`api/runs/${encodeURIComponent(runId)}`);
  }

  async function cancelRun(runId) {
    return jsonFetch(`api/runs/${encodeURIComponent(runId)}`, { method: "DELETE" });
  }

  /**
   * Long-poll the run's log endpoint until the run reaches a terminal status
   * (done | failed | cancelled). Calls back into the supplied handlers as
   * lines and status changes arrive.
   *
   * Returns a function the caller can invoke to stop polling early.
   */
  function streamRun(runId, handlers) {
    const { onLine, onStatus, onDone, onError } = handlers || {};
    let cancelled = false;
    let cursor = 0;
    let lastStatus = null;

    (async () => {
      while (!cancelled) {
        let resp;
        try {
          resp = await jsonFetch(`api/runs/${encodeURIComponent(runId)}/log?since=${cursor}&waitMs=8000`);
        } catch (e) {
          if (cancelled) return;
          if (onError) onError(e);
          // Back off a bit on network errors.
          await sleep(1500);
          continue;
        }
        cursor = (typeof resp.cursor === "number") ? resp.cursor : cursor;
        const lines = Array.isArray(resp.lines) ? resp.lines : [];
        if (onLine && lines.length) lines.forEach(l => onLine(l));
        if (resp.status !== lastStatus) {
          lastStatus = resp.status;
          if (onStatus) onStatus(resp.status, resp);
        }
        if (resp.status && resp.status !== "running" && resp.status !== "pending") {
          if (onDone) onDone(resp);
          return;
        }
      }
    })();

    return () => { cancelled = true; };
  }

  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  return {
    health, getConfig, persistConfig,
    listInputs, mkdirInput, uploadCsv,
    startSingle, startAuto, getRunStatus, cancelRun, streamRun,
  };
})();
