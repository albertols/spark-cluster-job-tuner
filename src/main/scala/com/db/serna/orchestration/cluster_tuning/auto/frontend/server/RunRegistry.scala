package com.db.serna.orchestration.cluster_tuning.auto.frontend.server

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable

/**
 * In-memory registry for tuner runs.
 *
 *   - Single-run gate: only one run executes at a time. If a second
 *     POST /api/runs/{single,auto} arrives mid-flight, the server returns
 *     409 with the current runId.
 *   - Per-run log buffer (capped) with a `since=N` cursor for long-poll.
 *   - Status enum: Pending → Running → Done | Failed | Cancelled.
 *
 * No disk persistence — restarting the server forgets prior runs. Re-discover
 * via the dashboard's existing `discoverAnalyses()` logic which reads the
 * outputs dir directly.
 */
object RunRegistry {

  sealed trait Status { def name: String }
  case object Pending   extends Status { val name = "pending" }
  case object Running   extends Status { val name = "running" }
  case object Done      extends Status { val name = "done" }
  case object Failed    extends Status { val name = "failed" }
  case object Cancelled extends Status { val name = "cancelled" }

  /** Append-only log buffer with a hard cap. */
  final class LogBuffer(maxLines: Int = 5000) {
    private val buf  = mutable.ArrayBuffer.empty[String]
    private val lock = new Object
    @volatile private var dropped: Int = 0

    /** Total appended lines including dropped — used as the "since" cursor. */
    def total: Int = lock.synchronized { dropped + buf.size }

    def append(line: String): Unit = lock.synchronized {
      if (buf.size >= maxLines) {
        // Evict oldest 10% to amortize cost.
        val evict = math.max(1, maxLines / 10)
        buf.remove(0, evict)
        dropped += evict
      }
      buf += line
    }

    /** Read all lines with absolute index >= `since`. Returns (newSince, lines). */
    def readSince(since: Int): (Int, Seq[String]) = lock.synchronized {
      val absStart = math.max(since, dropped)
      val rel      = absStart - dropped
      if (rel >= buf.size) (total, Seq.empty)
      else {
        val out = buf.iterator.slice(rel, buf.size).toVector
        (total, out)
      }
    }
  }

  final class Run(
    val runId: String,
    val mode: String,                     // "single" | "auto"
    val params: collection.Map[String, Any],
    val startedAt: Instant
  ) {
    val log: LogBuffer = new LogBuffer()
    private val statusRef    = new AtomicReference[Status](Pending)
    private val errorRef     = new AtomicReference[Option[String]](None)
    private val finishedAtRef = new AtomicReference[Option[Instant]](None)
    @volatile private var threadOpt: Option[Thread] = None

    def status: Status = statusRef.get()
    def error: Option[String] = errorRef.get()
    def finishedAt: Option[Instant] = finishedAtRef.get()
    def thread: Option[Thread] = threadOpt

    def setRunning(t: Thread): Unit = {
      threadOpt = Some(t)
      statusRef.set(Running)
    }
    def markDone(): Unit = {
      finishedAtRef.set(Some(Instant.now()))
      statusRef.set(Done)
    }
    def markFailed(reason: String): Unit = {
      errorRef.set(Some(reason))
      finishedAtRef.set(Some(Instant.now()))
      statusRef.set(Failed)
    }
    def markCancelled(): Unit = {
      finishedAtRef.set(Some(Instant.now()))
      statusRef.set(Cancelled)
    }

    def toJsonMap: mutable.LinkedHashMap[String, Any] = {
      val m = mutable.LinkedHashMap.empty[String, Any]
      m.put("runId", runId)
      m.put("mode", mode)
      m.put("status", status.name)
      m.put("startedAt", startedAt.toString)
      finishedAt.foreach(t => m.put("finishedAt", t.toString))
      error.foreach(e => m.put("error", e))
      m.put("logTotal", log.total)
      m
    }
  }

  // ── Registry state ────────────────────────────────────────────────────────

  private val runs       = new java.util.concurrent.ConcurrentHashMap[String, Run]()
  private val activeRef  = new AtomicReference[Option[Run]](None)
  private val seq        = new AtomicInteger(0)

  def active: Option[Run] = activeRef.get()
  def get(runId: String): Option[Run] = Option(runs.get(runId))

  /** Reserve the gate. Returns Right(run) if free, Left(currentActive) if busy. */
  def tryClaim(mode: String, params: collection.Map[String, Any]): Either[Run, Run] = {
    val candidate = new Run(
      runId = newRunId(mode),
      mode = mode,
      params = params,
      startedAt = Instant.now()
    )
    if (activeRef.compareAndSet(None, Some(candidate))) {
      runs.put(candidate.runId, candidate)
      Right(candidate)
    } else {
      Left(activeRef.get().get)
    }
  }

  /** Release the gate. Idempotent. */
  def release(run: Run): Unit = {
    activeRef.compareAndSet(Some(run), None)
  }

  private def newRunId(mode: String): String = {
    val n = seq.incrementAndGet()
    val ts = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
      .withZone(java.time.ZoneOffset.UTC)
      .format(Instant.now())
    s"$mode-$ts-$n"
  }
}
