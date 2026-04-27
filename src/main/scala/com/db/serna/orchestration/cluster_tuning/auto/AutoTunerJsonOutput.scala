package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.Json

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

/**
 * Produces analysis JSON and CSV outputs for the auto-tuner.
 *
 * Output files:
 *   _auto_tuner_analysis.json  — fleet-wide analysis (frontend-ready)
 *   _correlations.csv          — metric correlation results
 *   _divergences.csv           — outlier divergence detection
 *   _trend_summary.csv         — per-(cluster, recipe) trend summary
 *
 * And at the outputs root (one level above each run):
 *   _analyses_index.json       — frontend landing-page index of every run
 */
object AutoTunerJsonOutput {

  import Json._

  def analysisOutputJson(
    referenceDate: String,
    currentDate: String,
    strategyName: String,
    trends: Seq[TrendAssessment],
    correlations: Seq[CorrelationResult],
    divergences: Seq[DivergenceResult],
    decisions: Seq[EvolutionDecision]
  ): String = {

    val trendCounts = trends.groupBy(_.trend.label).mapValues(_.size)

    val metadata = obj(
      "generated_at" -> str(java.time.Instant.now().toString),
      "reference_date" -> str(referenceDate),
      "current_date" -> str(currentDate),
      "total_clusters" -> num(trends.map(_.cluster).distinct.size),
      "total_recipes" -> num(trends.size),
      "strategy" -> str(strategyName)
    )

    val trendsSummary = obj(
      "improved" -> num(trendCounts.getOrElse("improved", 0)),
      "degraded" -> num(trendCounts.getOrElse("degraded", 0)),
      "stable" -> num(trendCounts.getOrElse("stable", 0)),
      "new_entries" -> num(trendCounts.getOrElse("new_entry", 0)),
      "dropped_entries" -> num(trendCounts.getOrElse("dropped_entry", 0))
    )

    val decisionsByCluster = decisions.groupBy(_.cluster)
    val clusterTrendsJson = trends.groupBy(_.cluster).toSeq.sortBy(_._1).map { case (clusterName, clusterTrends) =>
      val clusterDecisions = decisionsByCluster.getOrElse(clusterName, Seq.empty)
      val overallTrend = determineOverallClusterTrend(clusterTrends)

      val recipesJson = clusterTrends.sortBy(_.recipe).map { t =>
        val decision = clusterDecisions.find(_.recipe == t.recipe)
        val deltasJson = t.deltas.map { d =>
          obj(
            "metric" -> str(d.metricName),
            "reference" -> num(formatValue(d.referenceValue)),
            "current" -> num(formatValue(d.currentValue)),
            "pct_change" -> num(formatValue(d.percentageChange))
          )
        }
        obj(
          "recipe" -> str(t.recipe),
          "trend" -> str(t.trend.label),
          "confidence" -> num(formatValue(t.confidenceLevel)),
          "action" -> str(decision.map(_.action.label).getOrElse("unknown")),
          "reason" -> str(decision.map(_.reason).getOrElse("")),
          "deltas" -> arr(deltasJson: _*)
        )
      }
      obj(
        "cluster" -> str(clusterName),
        "overall_trend" -> str(overallTrend),
        "recipes" -> arr(recipesJson: _*)
      )
    }

    val correlationsJson = correlations.map { c =>
      obj(
        "metric_a" -> str(c.metricA),
        "metric_b" -> str(c.metricB),
        "pearson" -> num(formatValue(c.pearsonCorrelation)),
        "covariance" -> num(formatValue(c.covariance)),
        "n" -> num(c.sampleSize)
      )
    }

    val divergencesJson = divergences.map { d =>
      obj(
        "cluster" -> str(d.cluster),
        "recipe" -> str(d.recipe),
        "metric" -> str(d.metricName),
        "reference" -> num(formatValue(d.referenceValue)),
        "current" -> num(formatValue(d.currentValue)),
        "z_score" -> num(formatValue(d.zScore)),
        "is_outlier" -> bool(d.isOutlier)
      )
    }

    Json.pretty(obj(
      "metadata" -> metadata,
      "trends_summary" -> trendsSummary,
      "cluster_trends" -> arr(clusterTrendsJson: _*),
      "correlations" -> arr(correlationsJson: _*),
      "divergences" -> arr(divergencesJson: _*)
    ))
  }

  def writeAnalysisCsvs(
    outDir: File,
    trends: Seq[TrendAssessment],
    correlations: Seq[CorrelationResult],
    divergences: Seq[DivergenceResult],
    decisions: Seq[EvolutionDecision]
  ): Unit = {
    writeTrendSummaryCsv(outDir, trends, decisions)
    writeCorrelationsCsv(outDir, correlations)
    writeDivergencesCsv(outDir, divergences)
  }

  private def writeTrendSummaryCsv(outDir: File, trends: Seq[TrendAssessment], decisions: Seq[EvolutionDecision]): Unit = {
    val decisionMap = decisions.map(d => (d.cluster, d.recipe) -> d).toMap
    val header = "cluster,recipe,trend,confidence,action,reason,p95_duration_ref,p95_duration_cur,p95_duration_pct_change"
    val rows = trends.sortBy(t => (t.cluster, t.recipe)).map { t =>
      val d = decisionMap.get((t.cluster, t.recipe))
      val p95 = t.deltas.find(_.metricName == "p95_job_duration_ms")
      Seq(
        t.cluster, t.recipe, t.trend.label,
        formatValue(t.confidenceLevel),
        d.map(_.action.label).getOrElse(""),
        d.map(_.reason).getOrElse("").replace(",", ";"),
        p95.map(d => formatValue(d.referenceValue)).getOrElse(""),
        p95.map(d => formatValue(d.currentValue)).getOrElse(""),
        p95.map(d => formatValue(d.percentageChange)).getOrElse("")
      ).mkString(",")
    }
    writeFile(outDir, "_trend_summary.csv", (header +: rows).mkString("\n"))
  }

  private def writeCorrelationsCsv(outDir: File, correlations: Seq[CorrelationResult]): Unit = {
    val header = "metric_a,metric_b,covariance,pearson_correlation,sample_size"
    val rows = correlations.map { c =>
      s"${c.metricA},${c.metricB},${formatValue(c.covariance)},${formatValue(c.pearsonCorrelation)},${c.sampleSize}"
    }
    writeFile(outDir, "_correlations.csv", (header +: rows).mkString("\n"))
  }

  private def writeDivergencesCsv(outDir: File, divergences: Seq[DivergenceResult]): Unit = {
    val header = "cluster,recipe,metric,reference_value,current_value,z_score,is_outlier"
    val rows = divergences.sortBy(d => -math.abs(d.zScore)).map { d =>
      s"${d.cluster},${d.recipe},${d.metricName},${formatValue(d.referenceValue)},${formatValue(d.currentValue)},${formatValue(d.zScore)},${d.isOutlier}"
    }
    writeFile(outDir, "_divergences.csv", (header +: rows).mkString("\n"))
  }

  private def writeFile(outDir: File, fileName: String, content: String): Unit = {
    if (!outDir.exists()) outDir.mkdirs()
    val f = new File(outDir, fileName)
    val bw = new BufferedWriter(new FileWriter(f))
    try bw.write(content) finally bw.close()
  }

  private def formatValue(d: Double): String = {
    if (d == d.toLong.toDouble) d.toLong.toString
    else f"$d%.4f"
  }

  private[auto] def determineOverallClusterTrend(trends: Seq[TrendAssessment]): String = {
    if (trends.exists(_.trend == Degraded)) "degraded"
    else if (trends.forall(_.trend == Improved)) "improved"
    else if (trends.exists(_.trend == Improved)) "mixed"
    else "stable"
  }

  /**
   * Rebuild `<outputsRoot>/_analyses_index.json` by scanning every sibling
   * directory that contains an `_auto_tuner_analysis.json` and extracting
   * its metadata. The index powers the frontend landing page.
   *
   * Format is stable and produced by this same object, so a light regex
   * extraction is enough — we avoid pulling in a JSON parser dependency.
   */
  def writeAnalysesIndex(outputsRoot: File): Unit = {
    if (!outputsRoot.isDirectory) return

    val children = Option(outputsRoot.listFiles()).getOrElse(Array.empty[File])
      .filter(_.isDirectory)
      .sortBy(_.getName)

    val entries = children.flatMap { dir =>
      val analysisFile = new File(dir, "_auto_tuner_analysis.json")
      if (analysisFile.isFile) readIndexEntry(dir.getName, analysisFile) else None
    }

    // Sort by current_date desc, then dir name desc as a tiebreaker.
    val sorted = entries.toSeq.sortBy(e => (e.currentDate, e.dir))(
      Ordering.Tuple2[String, String].reverse
    )

    val entriesJson = sorted.map { e =>
      obj(
        "dir" -> str(e.dir),
        "reference_date" -> str(e.referenceDate),
        "current_date" -> str(e.currentDate),
        "strategy" -> str(e.strategy),
        "total_clusters" -> num(e.totalClusters),
        "total_recipes" -> num(e.totalRecipes),
        "trends" -> obj(
          "improved" -> num(e.improved),
          "degraded" -> num(e.degraded),
          "stable" -> num(e.stable),
          "new_entries" -> num(e.newEntries),
          "dropped_entries" -> num(e.droppedEntries)
        ),
        "generated_at" -> str(e.generatedAt)
      )
    }

    val doc = Json.pretty(obj(
      "generated_at" -> str(java.time.Instant.now().toString),
      "entries" -> arr(entriesJson: _*)
    ))
    writeFile(outputsRoot, "_analyses_index.json", doc)
  }

  private case class AnalysisIndexEntry(
    dir: String,
    referenceDate: String,
    currentDate: String,
    strategy: String,
    totalClusters: Int,
    totalRecipes: Int,
    improved: Int,
    degraded: Int,
    stable: Int,
    newEntries: Int,
    droppedEntries: Int,
    generatedAt: String
  )

  private def readIndexEntry(dirName: String, analysisFile: File): Option[AnalysisIndexEntry] = {
    val src = Source.fromFile(analysisFile, "UTF-8")
    val body = try src.mkString finally src.close()

    // These fields are guaranteed to exist — we wrote the file ourselves.
    for {
      refDate <- extractStr(body, "reference_date")
      curDate <- extractStr(body, "current_date")
    } yield AnalysisIndexEntry(
      dir = dirName,
      referenceDate = refDate,
      currentDate = curDate,
      strategy = extractStr(body, "strategy").getOrElse("unknown"),
      totalClusters = extractInt(body, "total_clusters").getOrElse(0),
      totalRecipes = extractInt(body, "total_recipes").getOrElse(0),
      improved = extractInt(body, "improved").getOrElse(0),
      degraded = extractInt(body, "degraded").getOrElse(0),
      stable = extractInt(body, "stable").getOrElse(0),
      newEntries = extractInt(body, "new_entries").getOrElse(0),
      droppedEntries = extractInt(body, "dropped_entries").getOrElse(0),
      generatedAt = extractStr(body, "generated_at").getOrElse("")
    )
  }

  private def extractStr(body: String, key: String): Option[String] = {
    val pat = ("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"").r
    pat.findFirstMatchIn(body).map(_.group(1))
  }

  private def extractInt(body: String, key: String): Option[Int] = {
    val pat = ("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*(-?\\d+)").r
    pat.findFirstMatchIn(body).map(_.group(1).toInt)
  }
}
