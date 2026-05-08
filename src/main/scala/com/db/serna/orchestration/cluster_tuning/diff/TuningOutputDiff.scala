package com.db.serna.orchestration.cluster_tuning.diff

import com.db.serna.orchestration.cluster_tuning.single.Csv

import java.io.{File, PrintWriter}
import scala.io.Source

/**
 * Diffs two tuner output directories (e.g. flattened_true vs flattened_false) and writes a human-readable summary plus
 * per-area CSVs.
 *
 * Usage: main(Array("2025_12_20")) → compares outputs/2025_12_20_flattened_true vs outputs/2025_12_20_flattened_false
 * main(Array("2025_12_20", "<pathA>", "<pathB>")) → compares arbitrary directories; date is only used for the default
 * output location main(Array("2025_12_20", "<pathA>", "<pathB>", "<outDir>"))
 *
 * Produces in <outDir>: _diff_summary.txt — top-level human-readable report _diff_file_inventory.csv — per-file
 * presence/equality _diff_clusters_summary.csv — per-cluster field-level diff from _clusters-summary.csv
 * _diff_generation_summary.csv — per-cluster field-level diff from _generation_summary.csv _diff_json_content.csv —
 * per-cluster-JSON field-level diff (clusterConf block)
 */
object TuningOutputDiff {

  private val OutputsRoot =
    new File("src/main/resources/composer/dwh/config/cluster_tuning/outputs")

  final case class DiffConfig(
      date: String,
      pathA: File,
      pathB: File,
      outDir: File
  )

  object DiffConfig {
    def fromArgs(args: Array[String]): DiffConfig = {
      require(args.nonEmpty, "At least the date (YYYY_MM_DD) argument is required.")
      val date = args(0)
      require(date.matches("\\d{4}_\\d{2}_\\d{2}"), "Date must be in YYYY_MM_DD format (e.g., 2025_12_20).")

      val pathA =
        if (args.length >= 2) new File(args(1))
        else new File(OutputsRoot, s"${date}_flattened_true")
      val pathB =
        if (args.length >= 3) new File(args(2))
        else new File(OutputsRoot, s"${date}_flattened_false")
      val outDir =
        if (args.length >= 4) new File(args(3))
        else new File(OutputsRoot, s"${date}_diff_flattened_true_vs_false")

      DiffConfig(date, pathA, pathB, outDir)
    }
  }

  def main(args: Array[String]): Unit = {
    val cfg = DiffConfig.fromArgs(args)
    run(cfg)
  }

  def run(cfg: DiffConfig): Unit = {
    require(cfg.pathA.isDirectory, s"Path A is not a directory: ${cfg.pathA.getPath}")
    require(cfg.pathB.isDirectory, s"Path B is not a directory: ${cfg.pathB.getPath}")
    if (!cfg.outDir.exists()) cfg.outDir.mkdirs()

    val inventory = compareFileInventory(cfg.pathA, cfg.pathB)
    val summaryDiff = compareCsv(
      csv(cfg.pathA, "_clusters-summary.csv"),
      csv(cfg.pathB, "_clusters-summary.csv"),
      keyColumn = "cluster_name"
    )
    val generationDiff = compareCsv(
      csv(cfg.pathA, "_generation_summary.csv"),
      csv(cfg.pathB, "_generation_summary.csv"),
      keyColumn = "cluster_name"
    )
    val jsonDiff = compareClusterJsons(cfg.pathA, cfg.pathB, inventory.inBoth)

    writeInventoryCsv(new File(cfg.outDir, "_diff_file_inventory.csv"), inventory)
    writeCsvDiffCsv(new File(cfg.outDir, "_diff_clusters_summary.csv"), summaryDiff)
    writeCsvDiffCsv(new File(cfg.outDir, "_diff_generation_summary.csv"), generationDiff)
    writeJsonDiffCsv(new File(cfg.outDir, "_diff_json_content.csv"), jsonDiff)
    writeSummaryReport(new File(cfg.outDir, "_diff_summary.txt"), cfg, inventory, summaryDiff, generationDiff, jsonDiff)

    // Echo the top-level summary to stdout so it's visible when run from IntelliJ/CLI.
    val reportText = Source.fromFile(new File(cfg.outDir, "_diff_summary.txt")).mkString
    println(reportText)
  }

  // ── File inventory ────────────────────────────────────────────────────────

  final case class FileInventory(
      onlyInA: Seq[String],
      onlyInB: Seq[String],
      inBoth: Seq[String],
      identical: Seq[String],
      differing: Seq[String]
  )

  private def compareFileInventory(a: File, b: File): FileInventory = {
    val setA = listAllFiles(a).toSet
    val setB = listAllFiles(b).toSet
    val onlyA = (setA -- setB).toSeq.sorted
    val onlyB = (setB -- setA).toSeq.sorted
    val both = (setA intersect setB).toSeq.sorted

    val (identical, differing) = both.partition { name =>
      filesByteEqual(new File(a, name), new File(b, name))
    }
    FileInventory(onlyA, onlyB, both, identical, differing)
  }

  private def listAllFiles(dir: File): Seq[String] = {
    val files = Option(dir.listFiles()).getOrElse(Array.empty)
    files.filter(_.isFile).map(_.getName).toSeq
  }

  private def filesByteEqual(a: File, b: File): Boolean = {
    if (a.length() != b.length()) return false
    val bufA = java.nio.file.Files.readAllBytes(a.toPath)
    val bufB = java.nio.file.Files.readAllBytes(b.toPath)
    java.util.Arrays.equals(bufA, bufB)
  }

  private def writeInventoryCsv(out: File, inv: FileInventory): Unit = {
    val pw = new PrintWriter(out)
    try {
      pw.println("file,presence,equal")
      inv.onlyInA.foreach(f => pw.println(s"$f,only_in_A,"))
      inv.onlyInB.foreach(f => pw.println(s"$f,only_in_B,"))
      inv.identical.foreach(f => pw.println(s"$f,in_both,true"))
      inv.differing.foreach(f => pw.println(s"$f,in_both,false"))
    } finally pw.close()
  }

  // ── CSV field-level diff ──────────────────────────────────────────────────

  final case class CsvRowDiff(key: String, column: String, valueA: String, valueB: String)
  final case class CsvDiff(
      fileName: String,
      missingA: Boolean,
      missingB: Boolean,
      keysOnlyInA: Seq[String],
      keysOnlyInB: Seq[String],
      fieldDiffs: Seq[CsvRowDiff]
  )

  private def csv(dir: File, name: String): (String, File) = name -> new File(dir, name)

  private def compareCsv(a: (String, File), b: (String, File), keyColumn: String): CsvDiff = {
    val (name, fileA) = a
    val (_, fileB) = b
    val missingA = !fileA.exists()
    val missingB = !fileB.exists()
    if (missingA || missingB) {
      return CsvDiff(name, missingA, missingB, Nil, Nil, Nil)
    }

    val rowsA = Csv.parse(fileA)
    val rowsB = Csv.parse(fileB)
    val byKeyA = rowsA.flatMap(r => r.get(keyColumn).map(_ -> r)).toMap
    val byKeyB = rowsB.flatMap(r => r.get(keyColumn).map(_ -> r)).toMap

    val onlyA = (byKeyA.keySet -- byKeyB.keySet).toSeq.sorted
    val onlyB = (byKeyB.keySet -- byKeyA.keySet).toSeq.sorted
    val common = (byKeyA.keySet intersect byKeyB.keySet).toSeq.sorted

    val diffs = common.flatMap { k =>
      val rA = byKeyA(k)
      val rB = byKeyB(k)
      val columns = (rA.keySet ++ rB.keySet - keyColumn).toSeq.sorted
      columns.flatMap { col =>
        val vA = rA.getOrElse(col, "")
        val vB = rB.getOrElse(col, "")
        if (vA != vB) Some(CsvRowDiff(k, col, vA, vB)) else None
      }
    }
    CsvDiff(name, missingA = false, missingB = false, onlyA, onlyB, diffs)
  }

  private def writeCsvDiffCsv(out: File, d: CsvDiff): Unit = {
    val pw = new PrintWriter(out)
    try {
      pw.println("key,category,column,value_A,value_B")
      d.keysOnlyInA.foreach(k => pw.println(s"${quote(k)},only_in_A,,,"))
      d.keysOnlyInB.foreach(k => pw.println(s"${quote(k)},only_in_B,,,"))
      d.fieldDiffs.foreach { r =>
        pw.println(s"${quote(r.key)},field_diff,${quote(r.column)},${quote(r.valueA)},${quote(r.valueB)}")
      }
    } finally pw.close()
  }

  // ── JSON (clusterConf block) diff ─────────────────────────────────────────

  // We don't have a JSON parser dependency, so compare the JSON files by a minimal
  // field extraction: look at the clusterConf top-level block and each of its key:value
  // pairs (num_workers, worker_machine_type, etc.). That's sufficient to surface the
  // operationally-meaningful differences between two runs.

  private val ClusterConfKeys = Seq(
    "num_workers",
    "master_machine_type",
    "worker_machine_type",
    "autoscaling_policy",
    "total_no_of_jobs",
    "cluster_max_total_memory_gb",
    "cluster_max_total_cores",
    "accumulated_max_total_memory_per_jobs_gb"
  )

  final case class JsonClusterDiff(
      fileName: String,
      fieldDiffs: Seq[CsvRowDiff] // reuse: key=field, column="", values from each
  )

  private def compareClusterJsons(a: File, b: File, common: Seq[String]): Seq[JsonClusterDiff] = {
    common.filter(_.endsWith(".json")).flatMap { name =>
      val fa = new File(a, name)
      val fb = new File(b, name)
      if (!fa.exists() || !fb.exists()) None
      else {
        val contentA = readFileToString(fa)
        val contentB = readFileToString(fb)
        if (contentA == contentB) None
        else {
          val diffs = ClusterConfKeys.flatMap { k =>
            val vA = extractClusterConfField(contentA, k)
            val vB = extractClusterConfField(contentB, k)
            if (vA != vB) Some(CsvRowDiff(name, k, vA.getOrElse(""), vB.getOrElse(""))) else None
          }
          // recipe count (top-level keys inside recipeSparkConf)
          val recipeCountA = countRecipes(contentA)
          val recipeCountB = countRecipes(contentB)
          val recipeDiff =
            if (recipeCountA != recipeCountB)
              Seq(CsvRowDiff(name, "recipe_count", recipeCountA.toString, recipeCountB.toString))
            else Nil
          val all = diffs ++ recipeDiff
          if (all.nonEmpty) Some(JsonClusterDiff(name, all))
          else Some(JsonClusterDiff(name, Seq(CsvRowDiff(name, "content_other", "differs", "differs"))))
        }
      }
    }
  }

  private def readFileToString(f: File): String = {
    val src = Source.fromFile(f)
    try src.mkString
    finally src.close()
  }

  private def extractClusterConfField(json: String, key: String): Option[String] = {
    val clusterConfIdx = json.indexOf("\"clusterConf\"")
    if (clusterConfIdx < 0) return None
    val recipeIdx = json.indexOf("\"recipeSparkConf\"", clusterConfIdx)
    val windowEnd = if (recipeIdx > 0) recipeIdx else json.length
    val window = json.substring(clusterConfIdx, windowEnd)
    val needle = "\"" + key + "\""
    val kIdx = window.indexOf(needle)
    if (kIdx < 0) return None
    val afterColon = window.indexOf(':', kIdx)
    if (afterColon < 0) return None
    // Grab until next comma or '}' at the same nesting.
    var i = afterColon + 1
    val sb = new StringBuilder
    var depth = 0
    var inStr = false
    var finished = false
    while (i < window.length && !finished) {
      val ch = window.charAt(i)
      if (!inStr) {
        if (ch == '"') { inStr = true; sb.append(ch) }
        else if (ch == '{' || ch == '[') { depth += 1; sb.append(ch) }
        else if (ch == '}' || ch == ']') {
          if (depth == 0) finished = true else { depth -= 1; sb.append(ch) }
        } else if (ch == ',' && depth == 0) finished = true
        else sb.append(ch)
      } else {
        sb.append(ch)
        if (ch == '"' && window.charAt(i - 1) != '\\') inStr = false
      }
      i += 1
    }
    Some(sb.toString().trim.stripPrefix("\"").stripSuffix("\""))
  }

  private def countRecipes(json: String): Int = {
    val idx = json.indexOf("\"recipeSparkConf\"")
    if (idx < 0) return 0
    val colon = json.indexOf(':', idx)
    if (colon < 0) return 0
    // Count top-level quoted keys inside the recipeSparkConf object by scanning until the matching brace.
    var i = colon + 1
    while (i < json.length && json.charAt(i).isWhitespace) i += 1
    if (i >= json.length || json.charAt(i) != '{') return 0
    i += 1
    var depth = 1
    var inStr = false
    var count = 0
    var atTopLevel = true
    while (i < json.length && depth > 0) {
      val ch = json.charAt(i)
      if (inStr) {
        if (ch == '"' && json.charAt(i - 1) != '\\') inStr = false
      } else {
        ch match {
          case '{' | '[' => depth += 1; atTopLevel = false
          case '}' | ']' => depth -= 1; if (depth == 1) atTopLevel = true
          case '"' =>
            inStr = true
            if (depth == 1 && atTopLevel) {
              // A quoted key directly under recipeSparkConf. Count it once (detected at opening quote).
              count += 1
              // Skip to the closing quote to avoid re-counting on the same string.
              var j = i + 1
              var closed = false
              while (j < json.length && !closed) {
                if (json.charAt(j) == '"' && json.charAt(j - 1) != '\\') closed = true
                j += 1
              }
              i = j - 1
              inStr = false
              atTopLevel = false // next we'll hit ':' then the value; treat us as not-at-key-pos until value closes
            }
          case ':' if depth == 1 => atTopLevel = false
          case ',' if depth == 1 => atTopLevel = true
          case _ =>
        }
      }
      i += 1
    }
    count
  }

  private def writeJsonDiffCsv(out: File, diffs: Seq[JsonClusterDiff]): Unit = {
    val pw = new PrintWriter(out)
    try {
      pw.println("file,field,value_A,value_B")
      diffs.foreach { d =>
        d.fieldDiffs.foreach { r =>
          pw.println(s"${quote(d.fileName)},${quote(r.column)},${quote(r.valueA)},${quote(r.valueB)}")
        }
      }
    } finally pw.close()
  }

  // ── Top-level report ──────────────────────────────────────────────────────

  private def writeSummaryReport(
      out: File,
      cfg: DiffConfig,
      inv: FileInventory,
      summaryDiff: CsvDiff,
      generationDiff: CsvDiff,
      jsonDiff: Seq[JsonClusterDiff]
  ): Unit = {
    val sb = new StringBuilder
    sb.append(s"# Tuning Output Diff Report\n")
    sb.append(s"# Date  : ${cfg.date}\n")
    sb.append(s"# PathA : ${cfg.pathA.getPath}\n")
    sb.append(s"# PathB : ${cfg.pathB.getPath}\n")
    sb.append(s"# OutDir: ${cfg.outDir.getPath}\n\n")

    sb.append("## File inventory\n")
    sb.append(s"- Total files in A    : ${inv.onlyInA.size + inv.inBoth.size}\n")
    sb.append(s"- Total files in B    : ${inv.onlyInB.size + inv.inBoth.size}\n")
    sb.append(s"- Only in A           : ${inv.onlyInA.size}\n")
    inv.onlyInA.take(20).foreach(f => sb.append(s"    * $f\n"))
    if (inv.onlyInA.size > 20) sb.append(s"    ... (${inv.onlyInA.size - 20} more)\n")
    sb.append(s"- Only in B           : ${inv.onlyInB.size}\n")
    inv.onlyInB.take(20).foreach(f => sb.append(s"    * $f\n"))
    if (inv.onlyInB.size > 20) sb.append(s"    ... (${inv.onlyInB.size - 20} more)\n")
    sb.append(s"- In both (identical) : ${inv.identical.size}\n")
    sb.append(s"- In both (differing) : ${inv.differing.size}\n\n")

    sb.append(s"## _clusters-summary.csv diff\n")
    appendCsvDiffSection(sb, summaryDiff)

    sb.append(s"## _generation_summary.csv diff\n")
    appendCsvDiffSection(sb, generationDiff)

    sb.append(s"## Cluster JSON content diff (clusterConf block + recipe count)\n")
    sb.append(s"- JSONs differing     : ${jsonDiff.size}\n")
    val jsonsWithFieldDiffs = jsonDiff.filter(_.fieldDiffs.exists(_.column != "content_other"))
    sb.append(s"- ...with surfaced fields: ${jsonsWithFieldDiffs.size}\n")
    jsonsWithFieldDiffs.take(50).foreach { d =>
      sb.append(s"  • ${d.fileName}\n")
      d.fieldDiffs.take(10).foreach { r =>
        sb.append(s"      ${r.column}: A=${r.valueA} | B=${r.valueB}\n")
      }
      if (d.fieldDiffs.size > 10) sb.append(s"      ... (${d.fieldDiffs.size - 10} more fields)\n")
    }
    if (jsonsWithFieldDiffs.size > 50) sb.append(s"  ... (${jsonsWithFieldDiffs.size - 50} more files)\n")
    sb.append("\n")

    sb.append("## Files written\n")
    sb.append(s"- ${new File(cfg.outDir, "_diff_summary.txt").getName}\n")
    sb.append(s"- ${new File(cfg.outDir, "_diff_file_inventory.csv").getName}\n")
    sb.append(s"- ${new File(cfg.outDir, "_diff_clusters_summary.csv").getName}\n")
    sb.append(s"- ${new File(cfg.outDir, "_diff_generation_summary.csv").getName}\n")
    sb.append(s"- ${new File(cfg.outDir, "_diff_json_content.csv").getName}\n")

    val pw = new PrintWriter(out)
    try pw.write(sb.toString())
    finally pw.close()
  }

  private def appendCsvDiffSection(sb: StringBuilder, d: CsvDiff): Unit = {
    if (d.missingA || d.missingB) {
      sb.append(s"- Skipped (missingA=${d.missingA}, missingB=${d.missingB})\n\n")
      return
    }
    sb.append(s"- Clusters only in A      : ${d.keysOnlyInA.size}\n")
    d.keysOnlyInA.take(20).foreach(k => sb.append(s"    * $k\n"))
    if (d.keysOnlyInA.size > 20) sb.append(s"    ... (${d.keysOnlyInA.size - 20} more)\n")
    sb.append(s"- Clusters only in B      : ${d.keysOnlyInB.size}\n")
    d.keysOnlyInB.take(20).foreach(k => sb.append(s"    * $k\n"))
    if (d.keysOnlyInB.size > 20) sb.append(s"    ... (${d.keysOnlyInB.size - 20} more)\n")
    val diffsByCluster = d.fieldDiffs.groupBy(_.key)
    sb.append(s"- Clusters with field diffs: ${diffsByCluster.size} (total field diffs: ${d.fieldDiffs.size})\n")
    diffsByCluster.toSeq.sortBy(_._1).take(30).foreach { case (cluster, rows) =>
      sb.append(s"  • $cluster\n")
      rows.take(6).foreach(r => sb.append(s"      ${r.column}: A=${r.valueA} | B=${r.valueB}\n"))
      if (rows.size > 6) sb.append(s"      ... (${rows.size - 6} more columns)\n")
    }
    if (diffsByCluster.size > 30) sb.append(s"  ... (${diffsByCluster.size - 30} more clusters)\n")
    sb.append("\n")
  }

  // ── CSV writing helper ────────────────────────────────────────────────────

  private def quote(s: String): String = {
    if (s == null) ""
    else if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
      "\"" + s.replace("\"", "\"\"") + "\""
    } else s
  }
}
