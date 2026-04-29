package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter

/**
 * Pure CSV writers for the OSS mock generator. Each `b*Csv` returns the file
 * content as a `String`; each `writeB*` wraps that with a small file-write
 * helper. Splitting like this lets `MockGenSpec` assert content without disk I/O.
 *
 * Schemas mirror the existing tuner loaders exactly:
 *   - b13              -> ClusterMachineAndRecipeTuner.loadFlattened (12 cols)
 *   - b1..b12          -> ClusterMachineAndRecipeTuner.loadFromIndividualCSVs (per-file headers verified against real exports)
 *   - b14              -> ClusterDiagnostics.loadExitCodes (cluster_name triple-quoted to match BigQuery export)
 *   - b16              -> b16_oom_job_driver_exceptions.csv shape (currently unconsumed by run() but emitted for parity)
 *   - b20              -> ClusterMachineAndRecipeTuner.loadClusterSpans (10 cols)
 *   - b21              -> ClusterMachineAndRecipeTuner.loadAutoscalerEvents (15 cols, only RECOMMENDING + non-NULL target are read)
 *
 * Notes:
 *   - DAG / timer maps live at canonical project-wide paths
 *     (`src/main/resources/composer/dwh/config/_dag_*.csv`); this generator does
 *     NOT write those to avoid clobbering real data. Mock clusters resolve to
 *     `UNKNOWN_DAG_ID`/`ZERO_TIMER` in summaries — the tuner handles this.
 *   - Free-text fields (b14 `msg`, b16 `latest_driver_message`) avoid commas
 *     because the project's `Csv.parse` splits on comma without honoring CSV
 *     quoting. Real exports DO contain commas in those fields, but real
 *     loaders don't read them as rich content (b14 ignores `msg`; b16 isn't
 *     consumed by `run()`).
 */
object MockGen {

  private val logger = LoggerFactory.getLogger(getClass)

  // Mirror BigQuery's CSV export format for TIMESTAMP: "YYYY-MM-DD HH:MM:SS.SSSSSS UTC".
  // The tuner's parseInstant accepts ISO-8601 too, but matching the real export keeps
  // round-tripping through BigQuery's CSV format and our parser identical.
  private val BqTsFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS' UTC'").withZone(java.time.ZoneOffset.UTC)

  private def fmtTs(i: Instant): String = BqTsFormatter.format(i)

  private def fmtNum(d: Double): String = {
    if (d == d.toLong) d.toLong.toString
    else                f"$d%.6f"
  }

  private def emptyOrNum[N](o: Option[N]): String = o.map(_.toString).getOrElse("")

  // ── b13 ────────────────────────────────────────────────────────────────────
  //
  // Header: cluster_name,recipe_filename,avg_executors_per_job,p95_run_max_executors,
  //         avg_job_duration_ms,p95_job_duration_ms,runs,seconds_at_cap,
  //         runs_reaching_cap,total_runs,fraction_reaching_cap,max_concurrent_jobs

  def b13Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder
    sb.append("cluster_name,recipe_filename,avg_executors_per_job,p95_run_max_executors,")
    sb.append("avg_job_duration_ms,p95_job_duration_ms,runs,seconds_at_cap,")
    sb.append("runs_reaching_cap,total_runs,fraction_reaching_cap,max_concurrent_jobs\n")
    for (c <- scenario.clusters; r <- c.recipes) {
      sb.append(c.name).append(',')
        .append(r.name).append(',')
        .append(fmtNum(r.avgExecutorsPerJob)).append(',')
        .append(fmtNum(r.p95RunMaxExecutors)).append(',')
        .append(fmtNum(r.avgJobDurationMs)).append(',')
        .append(fmtNum(r.p95JobDurationMs)).append(',')
        .append(r.runs).append(',')
        .append(emptyOrNum(r.secondsAtCap)).append(',')
        .append(emptyOrNum(r.runsReachingCap)).append(',')
        .append(emptyOrNum(r.totalRuns)).append(',')
        .append(r.fractionReachingCap.map(fmtNum).getOrElse("")).append(',')
        .append(emptyOrNum(r.maxConcurrentJobs))
        .append('\n')
    }
    sb.toString
  }

  // ── Individual b1..b12 ────────────────────────────────────────────────────
  //
  // Same data as b13, refactored across files. Per-recipe files (b1, b3, b4,
  // b5, b8, b9, b10, b12) emit one row per (cluster, recipe). Per-cluster
  // files (b2, b6, b7, b11) emit one row per cluster, aggregating recipes.

  def b1Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,avg_executors_per_job\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',').append(fmtNum(r.avgExecutorsPerJob)).append('\n')
    sb.toString
  }

  def b2Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,peak_executors_seen\n")
    for (c <- scenario.clusters) {
      val peak = if (c.recipes.isEmpty) 0 else c.recipes.map(_.p95RunMaxExecutors).max.toInt
      sb.append(c.name).append(',').append(peak).append('\n')
    }
    sb.toString
  }

  def b3Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,avg_job_duration_ms\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',').append(fmtNum(r.avgJobDurationMs)).append('\n')
    sb.toString
  }

  def b4Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,peak_job_duration_ms\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',').append(fmtNum(r.p95JobDurationMs)).append('\n')
    sb.toString
  }

  def b5Csv(scenario: MockScenario): String = {
    // Real-export schema is 5 cols (no seconds_at_cap; that's in b9).
    val sb = new StringBuilder("cluster_name,recipe_filename,runs_reaching_cap,total_runs,fraction_reaching_cap\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',')
        .append(emptyOrNum(r.runsReachingCap)).append(',')
        .append(emptyOrNum(r.totalRuns)).append(',')
        .append(r.fractionReachingCap.map(fmtNum).getOrElse("")).append('\n')
    sb.toString
  }

  def b6Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,total_jobs\n")
    for (c <- scenario.clusters) {
      val totalJobs = c.recipes.map(_.runs).sum
      sb.append(c.name).append(',').append(totalJobs).append('\n')
    }
    sb.toString
  }

  def b7Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,total_runtime_ms,total_runtime_minutes,total_runtime_hours\n")
    for (c <- scenario.clusters) {
      val ms = c.recipes.map(r => r.avgJobDurationMs * r.runs).sum
      val min = ms / 60000.0
      val hr  = ms / 3600000.0
      sb.append(c.name).append(',').append(fmtNum(ms)).append(',')
        .append(fmtNum(min)).append(',').append(fmtNum(hr)).append('\n')
    }
    sb.toString
  }

  def b8Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,p95_job_duration_ms,avg_job_duration_ms,runs\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',')
        .append(fmtNum(r.p95JobDurationMs)).append(',')
        .append(fmtNum(r.avgJobDurationMs)).append(',')
        .append(r.runs).append('\n')
    sb.toString
  }

  def b9Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,seconds_at_cap\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',')
        .append(emptyOrNum(r.secondsAtCap)).append('\n')
    sb.toString
  }

  def b10Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,executors_added,executors_removed\n")
    for (c <- scenario.clusters; r <- c.recipes) {
      // Synthetic churn proxy — real loader doesn't currently consume b10.
      val adds = (r.avgExecutorsPerJob * r.runs).toLong
      sb.append(c.name).append(',').append(r.name).append(',')
        .append(adds).append(',').append(adds).append('\n')
    }
    sb.toString
  }

  def b11Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,max_concurrent_jobs\n")
    for (c <- scenario.clusters) {
      val maxConc = c.recipes.flatMap(_.maxConcurrentJobs).fold(1)(math.max)
      sb.append(c.name).append(',').append(maxConc).append('\n')
    }
    sb.toString
  }

  def b12Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("cluster_name,recipe_filename,p95_run_max_executors,avg_run_max_executors,runs\n")
    for (c <- scenario.clusters; r <- c.recipes)
      sb.append(c.name).append(',').append(r.name).append(',')
        .append(fmtNum(r.p95RunMaxExecutors)).append(',')
        .append(fmtNum(r.avgExecutorsPerJob)).append(',')
        .append(r.runs).append('\n')
    sb.toString
  }

  // ── b14 ────────────────────────────────────────────────────────────────────
  //
  // Header: timestamp,job_id,cluster_name,driver_exit_code,msg
  // cluster_name is wrapped as `"""<name>"""` to mimic the BigQuery JSON export
  // (the real loader strips all `"` chars). msg is plain text without commas
  // because the project's CSV parser splits on comma.

  def b14Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder("timestamp,job_id,cluster_name,driver_exit_code,msg\n")
    for (c <- scenario.clusters; e <- c.driverExitCodes) {
      val safeMsg = e.msg.replace(',', ' ').replace('"', '\'').replace('\n', ' ')
      sb.append(e.ts.toString).append(',')
        .append(e.jobId).append(',')
        .append("\"\"\"").append(c.name).append("\"\"\"").append(',')
        .append(e.exitCode).append(',')
        .append(safeMsg).append('\n')
    }
    sb.toString
  }

  // ── b16 ────────────────────────────────────────────────────────────────────
  //
  // Header: job_id,cluster_name,recipe_filename,latest_driver_log_ts,
  //         latest_driver_log_severity,latest_driver_log_class,
  //         latest_driver_exception_type,is_lost_task,is_stack_overflow,
  //         is_java_heap,latest_driver_message,log_name

  def b16Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder
    sb.append("job_id,cluster_name,recipe_filename,latest_driver_log_ts,")
    sb.append("latest_driver_log_severity,latest_driver_log_class,")
    sb.append("latest_driver_exception_type,is_lost_task,is_stack_overflow,")
    sb.append("is_java_heap,latest_driver_message,log_name\n")
    for (c <- scenario.clusters; o <- c.oomEvents) {
      val safeMsg = o.message.replace(',', ' ').replace('"', '\'').replace('\n', ' ')
      sb.append(o.jobId).append(',')
        .append(c.name).append(',')
        .append(o.recipe).append(',')
        .append(o.ts.toString).append(',')
        .append(o.severity).append(',')
        .append(o.driverClass).append(',')
        .append(o.exceptionType).append(',')
        .append(o.isLostTask).append(',')
        .append(o.isStackOverflow).append(',')
        .append(o.isJavaHeap).append(',')
        .append(safeMsg).append(',')
        .append(o.logName).append('\n')
    }
    sb.toString
  }

  // ── b20 ────────────────────────────────────────────────────────────────────
  //
  // One row per cluster incarnation. All flags emitted as lowercase booleans
  // ("true"/"false") to match the real BigQuery export.

  def b20Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder
    sb.append("cluster_name,incarnation_idx,span_start_ts,span_end_ts,span_minutes,")
    sb.append("create_event_ts,delete_event_ts,has_explicit_create,has_explicit_delete,total_events\n")
    for (c <- scenario.clusters) {
      c.incarnations.zipWithIndex.foreach { case (inc, i) =>
        val idx          = i + 1
        val spanMinutes  = (inc.spanEnd.toEpochMilli - inc.spanStart.toEpochMilli) / 60000.0
        val totalEvents  = inc.autoscaler.map(_.schedule.size + 2).getOrElse(0) // schedule + initial + final markers (synthetic)
        val createTs     = if (inc.hasExplicitCreate) fmtTs(inc.spanStart) else ""
        val deleteTs     = if (inc.hasExplicitDelete) fmtTs(inc.spanEnd)   else ""
        sb.append(c.name).append(',')
          .append(idx).append(',')
          .append(fmtTs(inc.spanStart)).append(',')
          .append(fmtTs(inc.spanEnd)).append(',')
          .append(fmtNum(spanMinutes)).append(',')
          .append(createTs).append(',')
          .append(deleteTs).append(',')
          .append(inc.hasExplicitCreate).append(',')
          .append(inc.hasExplicitDelete).append(',')
          .append(totalEvents)
          .append('\n')
      }
    }
    sb.toString
  }

  // ── b21 ────────────────────────────────────────────────────────────────────
  //
  // One RECOMMENDING row per autoscaler decision. Only events with state =
  // RECOMMENDING and non-NULL target_primary_workers are read by the tuner;
  // we emit only these (no COOLDOWN / SCALING / etc.) to keep output focused
  // and the loader filter trivial. Events fall strictly inside their
  // incarnation's span.

  def b21Csv(scenario: MockScenario): String = {
    val sb = new StringBuilder
    sb.append("cluster_name,event_ts,state,decision,decision_metric,")
    sb.append("current_primary_workers,target_primary_workers,")
    sb.append("min_primary_workers,max_primary_workers,")
    sb.append("current_secondary_workers,target_secondary_workers,")
    sb.append("min_secondary_workers,max_secondary_workers,")
    sb.append("recommendation_id,status_details\n")
    for (c <- scenario.clusters; inc <- c.incarnations; auto <- inc.autoscaler) {
      var prev: Int = auto.initialPrimary
      auto.schedule.zipWithIndex.foreach { case ((offSec, target), i) =>
        val ts = inc.spanStart.plusSeconds(offSec)
        // Clip to span boundary if a scenario inadvertently overshoots.
        val tsClipped = if (ts.isAfter(inc.spanEnd)) inc.spanEnd else ts
        val decision = if (target > prev) "SCALE_UP" else if (target < prev) "SCALE_DOWN" else "NO_SCALE"
        val recId    = f"mock-rec-${c.name}%s-${i + 1}%04d"
        sb.append(c.name).append(',')
          .append(fmtTs(tsClipped)).append(',')
          .append("RECOMMENDING").append(',')
          .append(decision).append(',')
          .append("YARN_MEMORY").append(',')
          .append(prev).append(',')
          .append(target).append(',')
          .append(auto.minPrimary).append(',')
          .append(auto.maxPrimary).append(',')
          .append("").append(',')   // current_secondary_workers
          .append("").append(',')   // target_secondary_workers
          .append("").append(',')   // min_secondary_workers
          .append("").append(',')   // max_secondary_workers
          .append(recId).append(',')
          .append("synthetic mock recommendation")
          .append('\n')
        prev = target
      }
    }
    sb.toString
  }

  // ── File-write wrappers ────────────────────────────────────────────────────

  /**
   * Write all input CSVs for a scenario into `dir` (which is created if absent).
   * Returns the list of files written, in the order produced.
   */
  def writeAll(scenario: MockScenario, dir: File): Seq[File] = {
    if (!dir.exists()) dir.mkdirs()
    val files = Seq(
      "b13_recommendations_inputs_per_recipe_per_cluster.csv" -> b13Csv(scenario),
      "b1_average_number_of_executors_per_job_by_cluster.csv" -> b1Csv(scenario),
      "b2_peak_executors_seen.csv"                            -> b2Csv(scenario),
      "b3_average_recipefilename_per_cluster.csv"             -> b3Csv(scenario),
      "b4_peak_job_duration_per_cluster.csv"                  -> b4Csv(scenario),
      "b5_a_times_job_reaches_max_executor_per_cluster.csv"   -> b5Csv(scenario),
      "b6_total_jobs_per_cluster.csv"                         -> b6Csv(scenario),
      "b7_total_runtime_all_jobs_per_cluster.csv"             -> b7Csv(scenario),
      "b8_P95_job_duration_per_recipe_per_cluster.csv"        -> b8Csv(scenario),
      "b9_time_at_cap_per_run_and_per_cluster.csv"            -> b9Csv(scenario),
      "b10_executor_churn_per_job_adds_removes.csv"           -> b10Csv(scenario),
      "b11_max_concurrent_jobs_per_cluster_in_window.csv"     -> b11Csv(scenario),
      "b12_p95_max_executors_per_recipe_per_cluster.csv"      -> b12Csv(scenario),
      "b14_clusters_with_nonzero_exit_codes.csv"              -> b14Csv(scenario),
      "b16_oom_job_driver_exceptions.csv"                     -> b16Csv(scenario),
      "b20_cluster_span_time.csv"                             -> b20Csv(scenario),
      "b21_cluster_autoscaler_values.csv"                     -> b21Csv(scenario)
    )
    val written = files.map { case (name, body) =>
      val f = new File(dir, name)
      writeString(f, body)
      f
    }
    logger.info(s"oss_mock: wrote ${written.size} CSVs to ${dir.getPath} (scenario='${scenario.name}', clusters=${scenario.clusters.size}, seed=${scenario.seed})")
    written
  }

  private def writeString(f: File, s: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(f))
    try bw.write(s) finally bw.close()
  }
}
