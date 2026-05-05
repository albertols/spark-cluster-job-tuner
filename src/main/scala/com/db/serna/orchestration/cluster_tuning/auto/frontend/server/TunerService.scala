package com.db.serna.orchestration.cluster_tuning.auto.frontend.server

import com.db.serna.orchestration.cluster_tuning.auto.{AutoTunerConf, ClusterMachineAndRecipeAutoTuner}
import com.db.serna.orchestration.cluster_tuning.single.{ClusterMachineAndRecipeTuner, ExecutorTopologyPreset, TuningStrategy, DefaultTuningStrategy}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.slf4j.LoggerFactory

import java.io.{File, IOException, InputStream}
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Embedded HTTP service that drives the dashboard's interactive wizard.
 *
 * Bind: 127.0.0.1 only (localhost dev tool — do not expose).
 *
 * Routes:
 *   GET    /api/health
 *   GET    /api/config            — read effective config (config.json + config.local.json overlay)
 *   PUT    /api/config            — write config.local.json (overlay only)
 *   GET    /api/inputs            — list date dirs under <inputsBase>
 *   POST   /api/inputs/<date>     — mkdir
 *   PUT    /api/inputs/<date>/csv?name=<bnn>.csv  — write CSV body
 *   POST   /api/runs/single       — { date, strategy, topology, flattened }
 *   POST   /api/runs/auto         — { ...AutoTunerConf options }
 *   GET    /api/runs/<runId>      — status snapshot
 *   GET    /api/runs/<runId>/log?since=N — long-poll log lines
 *   DELETE /api/runs/<runId>      — interrupt
 *
 * Otherwise: static files from the frontend directory.
 *
 * Modes:
 *   --server        (default) start HTTP server
 *   --cli single|auto [args]    — one-shot CLI; mirrors the original main()
 *                                  signatures so `java -jar tuner.jar --cli`
 *                                  works from CI/cron.
 */
object TunerService {

  private val logger = LoggerFactory.getLogger(getClass)

  // ── Configuration ─────────────────────────────────────────────────────────

  case class ServiceConfig(
    host: String = "127.0.0.1",
    port: Int    = 8080,
    frontendDir: File,
    inputsBase:  File,
    outputsBase: File,
    basePath:    String  // sets -DclusterTuning.basePath for the tuners
  )

  // ── Main ──────────────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val mode = args.headOption.getOrElse("--server")
    mode match {
      case "--server" | "server" => runServer(args.drop(if (args.headOption.contains("--server") || args.headOption.contains("server")) 1 else 0))
      case "--cli"    | "cli"    => runCli(args.drop(1))
      case "--help"   | "-h"     => printHelp(); sys.exit(0)
      case other =>
        // No mode flag — assume server mode; the rest are server flags.
        if (other.startsWith("--")) runServer(args)
        else { System.err.println(s"Unknown mode: $other"); printHelp(); sys.exit(1) }
    }
  }

  private def printHelp(): Unit = {
    val msg =
      """TunerService — Spark Cluster Tuner HTTP service + CLI.
        |
        |Usage:
        |  java -jar tuner-service.jar [--server] [--port=8080] [--host=127.0.0.1]
        |                              [--frontend-dir=PATH] [--inputs-dir=PATH] [--outputs-dir=PATH]
        |                              [--base-path=PATH]
        |  java -jar tuner-service.jar --cli single  YYYY_MM_DD [flattened=false] [--strategy=…] [--topology=…]
        |  java -jar tuner-service.jar --cli auto    --reference-date=… --current-date=… [--strategy=…] …
        |
        |--frontend-dir defaults to ./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend
        |--base-path defaults to ./src/main/resources/composer/dwh/config/cluster_tuning
        |--inputs-dir / --outputs-dir default to <base-path>/inputs and <base-path>/outputs
        |""".stripMargin
    println(msg)
  }

  // ── Server mode ──────────────────────────────────────────────────────────

  private def runServer(args: Array[String]): Unit = {
    val cfg = parseServerArgs(args)
    if (!cfg.frontendDir.isDirectory) {
      System.err.println(s"Frontend directory not found: ${cfg.frontendDir.getAbsolutePath}")
      sys.exit(2)
    }
    if (!cfg.inputsBase.isDirectory)  cfg.inputsBase.mkdirs()
    if (!cfg.outputsBase.isDirectory) cfg.outputsBase.mkdirs()

    // Tuners read this property whenever they construct paths.
    System.setProperty("clusterTuning.basePath", cfg.basePath)

    val server = HttpServer.create(new InetSocketAddress(cfg.host, cfg.port), 0)
    server.setExecutor(boundedExecutor("tuner-http", 8))
    server.createContext("/api/", new ApiHandler(cfg))
    server.createContext("/",     new StaticHandler(cfg.frontendDir))
    server.start()

    val rootUrl = s"http://${cfg.host}:${cfg.port}/"
    logger.info(s"TunerService listening on $rootUrl  (frontend=${cfg.frontendDir}, inputs=${cfg.inputsBase}, outputs=${cfg.outputsBase})")
    println(s"Open the dashboard at $rootUrl  ·  Ctrl+C to stop.")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Shutting down TunerService…")
      server.stop(2)
    }))

    Thread.currentThread().join() // wait until killed
  }

  private def parseServerArgs(args: Array[String]): ServiceConfig = {
    val argMap: Map[String, String] = args.iterator
      .filter(_.startsWith("--"))
      .map(_.stripPrefix("--"))
      .map { kv =>
        val i = kv.indexOf('=')
        if (i < 0) (kv, "true") else (kv.substring(0, i), kv.substring(i + 1))
      }
      .toMap

    val cwd = new File(".").getCanonicalFile
    val defaultFrontend = new File(cwd, "src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend")
    val defaultBase     = "src/main/resources/composer/dwh/config/cluster_tuning"
    val basePathArg     = argMap.getOrElse("base-path", defaultBase)
    val baseDir         = new File(basePathArg)
    val absBase         = if (baseDir.isAbsolute) baseDir else new File(cwd, basePathArg)
    val inputsBase      = argMap.get("inputs-dir").map(new File(_)).getOrElse(new File(absBase, "inputs"))
    val outputsBase     = argMap.get("outputs-dir").map(new File(_)).getOrElse(new File(absBase, "outputs"))

    ServiceConfig(
      host        = argMap.getOrElse("host", "127.0.0.1"),
      port        = argMap.get("port").map(_.toInt).getOrElse(8080),
      frontendDir = argMap.get("frontend-dir").map(new File(_)).getOrElse(defaultFrontend),
      inputsBase  = inputsBase,
      outputsBase = outputsBase,
      basePath    = absBase.getAbsolutePath
    )
  }

  // ── CLI mode ─────────────────────────────────────────────────────────────

  private def runCli(args: Array[String]): Unit = args.headOption match {
    case Some("single") => ClusterMachineAndRecipeTuner.main(args.drop(1))
    case Some("auto")   => ClusterMachineAndRecipeAutoTuner.main(args.drop(1))
    case _ =>
      System.err.println("Usage: --cli single|auto [args...]")
      printHelp()
      sys.exit(1)
  }

  // ── Static file handler ──────────────────────────────────────────────────

  private final class StaticHandler(root: File) extends HttpHandler {
    private val rootCanon = root.getCanonicalFile
    override def handle(ex: HttpExchange): Unit = withErrorHandling(ex) {
      val rawPath = ex.getRequestURI.getPath
      val rel = rawPath.stripPrefix("/")
      val file =
        if (rel.isEmpty) new File(rootCanon, "index.html")
        else new File(rootCanon, URLDecoder.decode(rel, StandardCharsets.UTF_8.name()))
      val canon = file.getCanonicalFile
      // Prevent path traversal.
      if (!canon.toPath.startsWith(rootCanon.toPath)) {
        sendText(ex, 403, "Forbidden")
        return
      }
      val target =
        if (canon.isDirectory) new File(canon, "index.html").getCanonicalFile
        else canon
      if (!target.isFile) {
        sendText(ex, 404, s"Not found: $rawPath")
        return
      }
      val mime = mimeFor(target.getName)
      val len  = target.length()
      ex.getResponseHeaders.set("Content-Type", mime)
      ex.getResponseHeaders.set("Cache-Control", "no-store")
      ex.sendResponseHeaders(200, len)
      val out = ex.getResponseBody
      try Files.copy(target.toPath, out) finally out.close()
    }
  }

  private def mimeFor(name: String): String = name.toLowerCase match {
    case n if n.endsWith(".html") => "text/html; charset=utf-8"
    case n if n.endsWith(".js")   => "application/javascript; charset=utf-8"
    case n if n.endsWith(".css")  => "text/css; charset=utf-8"
    case n if n.endsWith(".json") => "application/json; charset=utf-8"
    case n if n.endsWith(".sql")  => "text/plain; charset=utf-8"
    case n if n.endsWith(".csv")  => "text/csv; charset=utf-8"
    case n if n.endsWith(".md")   => "text/markdown; charset=utf-8"
    case n if n.endsWith(".png")  => "image/png"
    case n if n.endsWith(".svg")  => "image/svg+xml"
    case n if n.endsWith(".ico")  => "image/x-icon"
    case _                        => "application/octet-stream"
  }

  // ── API handler ──────────────────────────────────────────────────────────

  private val DateRe = "^\\d{4}_\\d{2}_\\d{2}$".r

  private final class ApiHandler(cfg: ServiceConfig) extends HttpHandler {
    override def handle(ex: HttpExchange): Unit = withErrorHandling(ex) {
      val method = ex.getRequestMethod.toUpperCase
      val path   = ex.getRequestURI.getPath
      val q      = parseQuery(ex.getRequestURI.getRawQuery)

      (method, path) match {
        case ("GET",  "/api/health")  => handleHealth(ex)
        case ("GET",  "/api/config")  => handleGetConfig(ex)
        case ("PUT",  "/api/config")  => handlePutConfig(ex)
        case ("GET",  "/api/inputs")  => handleListInputs(ex)
        case ("POST", p) if p.startsWith("/api/inputs/") && !p.endsWith("/csv") =>
          val date = p.stripPrefix("/api/inputs/").stripSuffix("/")
          handleMkdirInput(ex, date)
        case ("PUT", p) if p.startsWith("/api/inputs/") && p.endsWith("/csv") =>
          val date = p.stripPrefix("/api/inputs/").stripSuffix("/csv").stripSuffix("/")
          handleUploadCsv(ex, date, q.getOrElse("name", ""))
        case ("POST", "/api/runs/single") => handleStartRun(ex, "single")
        case ("POST", "/api/runs/auto")   => handleStartRun(ex, "auto")
        case ("GET",  p) if p.startsWith("/api/runs/") && p.endsWith("/log") =>
          val runId = p.stripPrefix("/api/runs/").stripSuffix("/log").stripSuffix("/")
          handleRunLog(ex, runId, q)
        case ("GET",  p) if p.startsWith("/api/runs/") =>
          val runId = p.stripPrefix("/api/runs/").stripSuffix("/")
          handleRunStatus(ex, runId)
        case ("DELETE", p) if p.startsWith("/api/runs/") =>
          val runId = p.stripPrefix("/api/runs/").stripSuffix("/")
          handleRunCancel(ex, runId)
        case _ => sendJson(ex, 404, errorJson(s"No route for $method $path"))
      }
    }

    // ── Routes ──────────────────────────────────────────────────────────────

    private def handleHealth(ex: HttpExchange): Unit = {
      val m = mutable.LinkedHashMap.empty[String, Any]
      m.put("ok", java.lang.Boolean.TRUE)
      m.put("frontendDir", cfg.frontendDir.getAbsolutePath)
      m.put("inputsBase",  cfg.inputsBase.getAbsolutePath)
      m.put("outputsBase", cfg.outputsBase.getAbsolutePath)
      RunRegistry.active.foreach(r => m.put("activeRunId", r.runId))
      sendJson(ex, 200, JsonIO.stringify(m))
    }

    private def handleGetConfig(ex: HttpExchange): Unit = {
      val baseFile  = new File(cfg.frontendDir, "config.json")
      val localFile = new File(cfg.frontendDir, "config.local.json")
      val merged = mutable.LinkedHashMap.empty[String, Any]
      if (baseFile.isFile) {
        val parsed = JsonIO.parseObject(readUtf8(baseFile))
        parsed.foreach { case (k, v) => merged.put(k, v) }
      }
      if (localFile.isFile) {
        try {
          val parsed = JsonIO.parseObject(readUtf8(localFile))
          parsed.foreach { case (k, v) => merged.put(k, v) }
        } catch { case NonFatal(e) => logger.warn(s"Ignoring bad config.local.json: ${e.getMessage}") }
      }
      sendJson(ex, 200, JsonIO.stringify(merged))
    }

    private def handlePutConfig(ex: HttpExchange): Unit = {
      val body  = readBody(ex)
      val patch = try JsonIO.parseObject(body) catch {
        case e: JsonIO.ParseException => sendJson(ex, 400, errorJson(s"Bad JSON: ${e.getMessage}")); return
      }
      val localFile = new File(cfg.frontendDir, "config.local.json")
      val current   =
        if (localFile.isFile) try JsonIO.parseObject(readUtf8(localFile)) catch { case _: Throwable => mutable.LinkedHashMap.empty[String, Any] }
        else mutable.LinkedHashMap.empty[String, Any]
      patch.foreach { case (k, v) => current.put(k, v) }
      val out = JsonIO.stringify(current, pretty = true) + "\n"
      writeUtf8(localFile, out)
      sendJson(ex, 200, JsonIO.stringify(current))
    }

    private def handleListInputs(ex: HttpExchange): Unit = {
      val out = mutable.ArrayBuffer.empty[Any]
      Option(cfg.inputsBase.listFiles()).getOrElse(Array.empty).filter(_.isDirectory).sortBy(_.getName).foreach { d =>
        val name = d.getName
        if (DateRe.findFirstIn(name).isDefined) {
          val files = Option(d.listFiles()).getOrElse(Array.empty)
            .filter(_.isFile).map(_.getName).sorted.toSeq
          val entry = mutable.LinkedHashMap.empty[String, Any]
          entry.put("name", name)
          entry.put("files", files)
          out += entry
        }
      }
      val resp = mutable.LinkedHashMap[String, Any]("dates" -> out)
      sendJson(ex, 200, JsonIO.stringify(resp))
    }

    private def handleMkdirInput(ex: HttpExchange, date: String): Unit = {
      if (!validDate(date)) { sendJson(ex, 400, errorJson("Bad date format (YYYY_MM_DD)")); return }
      val dir = new File(cfg.inputsBase, date)
      if (!dir.exists()) dir.mkdirs()
      val resp = mutable.LinkedHashMap[String, Any]("date" -> date, "path" -> dir.getAbsolutePath)
      sendJson(ex, 201, JsonIO.stringify(resp))
    }

    private def handleUploadCsv(ex: HttpExchange, date: String, name: String): Unit = {
      if (!validDate(date)) { sendJson(ex, 400, errorJson("Bad date format (YYYY_MM_DD)")); return }
      if (!validCsvName(name)) { sendJson(ex, 400, errorJson(s"Bad csv filename '$name'")); return }
      val dir = new File(cfg.inputsBase, date)
      if (!dir.exists()) dir.mkdirs()
      val target = new File(dir, name).toPath
      val tmp    = new File(dir, name + ".part").toPath
      val in: InputStream = ex.getRequestBody
      try Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING) finally in.close()
      Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
      val resp = mutable.LinkedHashMap[String, Any]("name" -> name, "bytes" -> Files.size(target).asInstanceOf[Any])
      sendJson(ex, 201, JsonIO.stringify(resp))
    }

    private def handleStartRun(ex: HttpExchange, mode: String): Unit = {
      val body = readBody(ex)
      val params = try JsonIO.parseObject(body) catch {
        case e: JsonIO.ParseException => sendJson(ex, 400, errorJson(s"Bad JSON: ${e.getMessage}")); return
      }

      // Validate dates up-front to give a useful error before claiming the gate.
      mode match {
        case "single" =>
          val date = JsonIO.str(params, "date")
          if (!validDate(date)) { sendJson(ex, 400, errorJson("Missing or bad 'date' (YYYY_MM_DD)")); return }
        case "auto" =>
          val ref = JsonIO.str(params, "referenceDate")
          val cur = JsonIO.str(params, "currentDate")
          if (!validDate(ref) || !validDate(cur)) {
            sendJson(ex, 400, errorJson("Missing or bad 'referenceDate' / 'currentDate' (YYYY_MM_DD)")); return
          }
      }

      RunRegistry.tryClaim(mode, params) match {
        case Left(active) =>
          val resp = mutable.LinkedHashMap[String, Any](
            "error" -> "run_in_progress",
            "currentRunId" -> active.runId,
            "currentMode"  -> active.mode
          )
          sendJson(ex, 409, JsonIO.stringify(resp))

        case Right(claimed) =>
          val t = new Thread(new Runnable {
            override def run(): Unit = executeRun(claimed, mode, params)
          }, s"tuner-${claimed.runId}")
          t.setDaemon(true)
          claimed.setRunning(t)
          t.start()
          sendJson(ex, 202, JsonIO.stringify(claimed.toJsonMap))
      }
    }

    private def handleRunStatus(ex: HttpExchange, runId: String): Unit = {
      RunRegistry.get(runId) match {
        case Some(run) => sendJson(ex, 200, JsonIO.stringify(run.toJsonMap))
        case None      => sendJson(ex, 404, errorJson(s"No such run: $runId"))
      }
    }

    private def handleRunLog(ex: HttpExchange, runId: String, q: Map[String, String]): Unit = {
      RunRegistry.get(runId) match {
        case None => sendJson(ex, 404, errorJson(s"No such run: $runId"))
        case Some(run) =>
          val since = q.get("since").flatMap(s => try Some(s.toInt) catch { case _: Throwable => None }).getOrElse(0)
          val waitMs = q.get("waitMs").flatMap(s => try Some(s.toLong) catch { case _: Throwable => None }).getOrElse(8000L).min(25000L)

          // Long-poll: if no new lines yet AND run is still running, sleep briefly.
          val deadline = System.currentTimeMillis() + waitMs
          var (cursor, lines) = run.log.readSince(since)
          while (lines.isEmpty && run.status == RunRegistry.Running && System.currentTimeMillis() < deadline) {
            Thread.sleep(200)
            val r = run.log.readSince(since); cursor = r._1; lines = r._2
          }
          val resp = mutable.LinkedHashMap.empty[String, Any]
          resp.put("runId", runId)
          resp.put("status", run.status.name)
          resp.put("cursor", java.lang.Integer.valueOf(cursor))
          resp.put("lines", lines)
          run.finishedAt.foreach(t => resp.put("finishedAt", t.toString))
          run.error.foreach(e => resp.put("error", e))
          sendJson(ex, 200, JsonIO.stringify(resp))
      }
    }

    private def handleRunCancel(ex: HttpExchange, runId: String): Unit = {
      RunRegistry.get(runId) match {
        case None => sendJson(ex, 404, errorJson(s"No such run: $runId"))
        case Some(run) =>
          run.thread.foreach { t =>
            t.interrupt()
            // The run thread's body wraps in try/catch and will mark cancelled.
          }
          run.markCancelled()
          RunRegistry.release(run)
          sendJson(ex, 202, JsonIO.stringify(run.toJsonMap))
      }
    }

    // ── Run executor ────────────────────────────────────────────────────────

    private def executeRun(run: RunRegistry.Run, mode: String, params: collection.Map[String, Any]): Unit = {
      try {
        RunLogAppender.withRunCapture(run.log) {
          run.log.append(s"[run ${run.runId}] mode=$mode params=${JsonIO.stringify(params)}")
          mode match {
            case "single" => runSingle(run, params)
            case "auto"   => runAuto(run, params)
            case other    => throw new IllegalArgumentException(s"Unknown mode: $other")
          }
        }
        run.markDone()
      } catch {
        case e: InterruptedException =>
          run.log.append(s"[run ${run.runId}] interrupted")
          run.markCancelled()
        case NonFatal(e) =>
          run.log.append(s"[run ${run.runId}] FAILED: ${e.toString}")
          val sw = new java.io.StringWriter; e.printStackTrace(new java.io.PrintWriter(sw))
          sw.toString.linesIterator.take(20).foreach(l => run.log.append("  " + l))
          run.markFailed(e.toString)
      } finally {
        RunRegistry.release(run)
      }
    }

    private def runSingle(run: RunRegistry.Run, params: collection.Map[String, Any]): Unit = {
      val date      = JsonIO.str(params, "date")
      val flat      = JsonIO.boolOpt(params, "flattened").getOrElse(true)
      val strategy  = JsonIO.strOpt(params, "strategy").flatMap(TuningStrategy.fromName).getOrElse(DefaultTuningStrategy)
      val topo      = JsonIO.strOpt(params, "topology").flatMap(ExecutorTopologyPreset.fromLabel)
      val effective = wrapTopology(strategy, topo)

      // Use applyAt so we feed the resolved inputs/outputs roots from config.
      val cfgObj = ClusterMachineAndRecipeTuner.Config.applyAt(
        useFlattened = flat,
        date = date,
        baseInputsDir = cfg.inputsBase,
        baseOutputsDir = cfg.outputsBase
      )
      run.log.append(s"[run] inputDir=${cfgObj.inputDir.getAbsolutePath}")
      run.log.append(s"[run] outputDir=${cfgObj.outputDir.getAbsolutePath}")
      ClusterMachineAndRecipeTuner.run(cfgObj, effective)
    }

    private def runAuto(run: RunRegistry.Run, params: collection.Map[String, Any]): Unit = {
      // Build a Scallop arg list from the JSON params; reuse AutoTunerConf
      // verbatim so default validation (range checks, regex) stays consistent.
      val args = mutable.ArrayBuffer.empty[String]
      args += s"--reference-date=${JsonIO.str(params, "referenceDate")}"
      args += s"--current-date=${JsonIO.str(params, "currentDate")}"
      JsonIO.strOpt(params, "strategy").foreach(s => args += s"--strategy=$s")
      JsonIO.numOpt(params, "b16ReboostingFactor").foreach(d => args += s"--b16-rebooting-factor=$d")
      JsonIO.numOpt(params, "b17ReboostingFactor").foreach(d => args += s"--b17-rebooting-factor=$d")
      JsonIO.numOpt(params, "divergenceZThreshold").foreach(d => args += s"--divergence-z-threshold=$d")
      JsonIO.numOpt(params, "executorScaleFactor").foreach(d => args += s"--executor-scale-factor=$d")
      JsonIO.numOpt(params, "scaleZThreshold").foreach(d => args += s"--scale-z-threshold=$d")
      JsonIO.numOpt(params, "scaleCapTouchRatio").foreach(d => args += s"--scale-cap-touch-ratio=$d")
      JsonIO.boolOpt(params, "keepHistoricalTuning").foreach { b =>
        if (b) args += "--keep-historical-tuning"
      }
      // Topology override is not in AutoTunerConf — surface via system property?
      // The auto tuner's strategy resolution uses TuningStrategy.fromName only;
      // topology is derived from the strategy. We surface a notice so users
      // know topology overrides aren't honoured by AutoTuner today (Phase 1+2 only
      // supports it on the single tuner).
      JsonIO.strOpt(params, "topology").foreach { t =>
        run.log.append(s"[run] note: --topology=$t is not propagated through AutoTunerConf; the strategy's preset is used.")
      }

      run.log.append(s"[run] args: ${args.mkString(" ")}")
      val conf = new AutoTunerConf(args.toArray[String])
      ClusterMachineAndRecipeAutoTuner.run(conf)
    }

    private def wrapTopology(base: TuningStrategy, topoOpt: Option[ExecutorTopologyPreset]): TuningStrategy = topoOpt match {
      case None => base
      case Some(topo) =>
        new TuningStrategy {
          val name                  = s"${base.name}+${topo.label}"
          val biasMode              = base.biasMode
          val executorTopology      = topo
          val machinePreference     = base.machinePreference
          val quotas                = base.quotas
          val capHitBoostPct        = base.capHitBoostPct
          val capHitThreshold       = base.capHitThreshold
          val preferMaxWorkers      = base.preferMaxWorkers
          val perWorkerPenaltyPct   = base.perWorkerPenaltyPct
          val memoryOverheadRatio   = base.memoryOverheadRatio
          val osAndDaemonsReserveGb = base.osAndDaemonsReserveGb
          val manualInstancesFrom   = base.manualInstancesFrom
          val minExecutorInstances  = base.minExecutorInstances
          val daMinFrom             = base.daMinFrom
          val daInitialEqualsMin    = base.daInitialEqualsMin
        }
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private def withErrorHandling(ex: HttpExchange)(body: => Unit): Unit = {
    try body
    catch {
      case e: IOException =>
        // Client disconnected mid-stream, etc. Don't dump to log.
        try ex.close() catch { case _: Throwable => () }
      case NonFatal(e) =>
        logger.error(s"Unhandled error in ${ex.getRequestMethod} ${ex.getRequestURI}", e)
        try sendJson(ex, 500, errorJson(s"Server error: ${e.toString}"))
        catch { case _: Throwable => () }
    }
  }

  private def parseQuery(raw: String): Map[String, String] = {
    if (raw == null || raw.isEmpty) return Map.empty
    raw.split("&").iterator.flatMap { kv =>
      val i = kv.indexOf('=')
      if (i < 0) None
      else {
        val k = URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8.name())
        val v = URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8.name())
        Some(k -> v)
      }
    }.toMap
  }

  private def sendJson(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    ex.getResponseHeaders.set("Cache-Control", "no-store")
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val out = ex.getResponseBody
    try out.write(bytes) finally out.close()
  }

  private def sendText(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.getResponseHeaders.set("Content-Type", "text/plain; charset=utf-8")
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val out = ex.getResponseBody
    try out.write(bytes) finally out.close()
  }

  private def errorJson(msg: String): String =
    JsonIO.stringify(mutable.LinkedHashMap[String, Any]("error" -> msg))

  private def readBody(ex: HttpExchange): String = {
    val baos = new java.io.ByteArrayOutputStream()
    val in = ex.getRequestBody
    val buf = new Array[Byte](8192)
    try {
      var n = in.read(buf)
      while (n > 0) { baos.write(buf, 0, n); n = in.read(buf) }
    } finally in.close()
    new String(baos.toByteArray, StandardCharsets.UTF_8)
  }

  private def readUtf8(f: File): String =
    new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)

  private def writeUtf8(f: File, content: String): Unit = {
    val parent = f.getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
    val tmp = new File(parent, f.getName + ".part").toPath
    Files.write(tmp, content.getBytes(StandardCharsets.UTF_8))
    Files.move(tmp, f.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }

  private def validDate(s: String): Boolean = s != null && DateRe.findFirstIn(s).isDefined
  private def validCsvName(s: String): Boolean =
    s != null && s.nonEmpty && !s.contains('/') && !s.contains('\\') && !s.contains("..") && s.endsWith(".csv")

  private def boundedExecutor(name: String, threads: Int): ExecutorService = {
    val tf: ThreadFactory = new ThreadFactory {
      private val seq = new java.util.concurrent.atomic.AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"$name-${seq.incrementAndGet()}")
        t.setDaemon(true)
        t
      }
    }
    Executors.newFixedThreadPool(threads, tf)
  }
}
