// Tuner API client.
//
// Phase 1 (current): the dashboard is served by python http.server (static
// only). All methods except `health()` throw — the wizard treats this as
// "API mode unavailable" and falls back to the guided-CLI flow.
//
// Phase 2 (future): a Scala TunerService HTTP server (JDK com.sun.net.httpserver)
// will boot via `serve.sh --api` and answer these endpoints. The wizard's
// Step 4 will gain a "Start Tuning" button that calls startSingle/startAuto
// and streams the run log.

const TunerApi = (() => {
  const NOT_AVAILABLE = "API mode not available — wizard is running against the static-file server. " +
    "Use the generated IntelliJ run config or shell command to invoke the tuner.";

  async function health() {
    try {
      const r = await fetch("api/health", { cache: "no-store" });
      if (!r.ok) return false;
      const j = await r.json();
      return !!(j && j.ok);
    } catch (_) {
      return false;
    }
  }

  async function uploadCsv(_date, _name, _file) { throw new Error(NOT_AVAILABLE); }
  async function startSingle(_args)             { throw new Error(NOT_AVAILABLE); }
  async function startAuto(_args)               { throw new Error(NOT_AVAILABLE); }
  async function streamRun(_runId, _onLine, _onDone) { throw new Error(NOT_AVAILABLE); }
  async function persistConfig(_patch)          { throw new Error(NOT_AVAILABLE); }

  return { health, uploadCsv, startSingle, startAuto, streamRun, persistConfig };
})();
