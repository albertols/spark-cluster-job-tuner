package com.db.serna.local.utils

import java.io.{File, PrintWriter}
import scala.io.Source

/**
 * Reads all CSVs in the input directory, collects every unique `cluster_name` and `recipe_filename` value across files,
 * and rewrites them with anonymised placeholders that are consistent across all CSVs. x
 */
object CleanClusterAndRecipeNames extends App {

  val inputDir = new File(
    "src/main/resources/composer/dwh/config/cluster_tuning/inputs/2025_12_20"
  )

  require(inputDir.isDirectory, s"Input directory not found: ${inputDir.getAbsolutePath}")

  val csvFiles: Array[File] = inputDir.listFiles().filter(_.getName.endsWith(".csv")).sorted

  // ── First pass: collect every unique value ──────────────────────────────────
  var allClusterNames = Set.empty[String]
  var allRecipeFilenames = Set.empty[String]

  for (file <- csvFiles) {
    val src = Source.fromFile(file, "UTF-8")
    val lines =
      try src.getLines().toList
      finally src.close()

    if (lines.nonEmpty) {
      val header = lines.head.split(",", -1)
      val clusterIdx = header.indexOf("cluster_name")
      val recipeIdx = header.indexOf("recipe_filename")

      for (line <- lines.tail if line.nonEmpty) {
        val cols = line.split(",", -1)
        if (clusterIdx >= 0 && clusterIdx < cols.length && cols(clusterIdx).nonEmpty)
          allClusterNames += cols(clusterIdx)
        if (recipeIdx >= 0 && recipeIdx < cols.length && cols(recipeIdx).nonEmpty)
          allRecipeFilenames += cols(recipeIdx)
      }
    }
  }

  // ── Build deterministic global mappings (sorted → numbered) ────────────────
  val clusterMapping: Map[String, String] =
    allClusterNames.toSeq.sorted.zipWithIndex.map { case (original, idx) =>
      original -> s"cluster-wf_spark_${idx + 1}"
    }.toMap

  val recipeMapping: Map[String, String] =
    allRecipeFilenames.toSeq.sorted.zipWithIndex.map { case (original, idx) =>
      original -> s"_SPARK_JOB_recipe_filename_${idx + 1}.json"
    }.toMap

  // ── Second pass: rewrite every CSV in-place ────────────────────────────────
  for (file <- csvFiles) {
    val src = Source.fromFile(file, "UTF-8")
    val lines =
      try src.getLines().toList
      finally src.close()

    if (lines.nonEmpty) {
      val header = lines.head.split(",", -1)
      val clusterIdx = header.indexOf("cluster_name")
      val recipeIdx = header.indexOf("recipe_filename")

      val rewritten = lines.head +: lines.tail.map { line =>
        if (line.isEmpty) line
        else {
          val cols = line.split(",", -1)
          if (clusterIdx >= 0 && clusterIdx < cols.length && cols(clusterIdx).nonEmpty)
            cols(clusterIdx) = clusterMapping(cols(clusterIdx))
          if (recipeIdx >= 0 && recipeIdx < cols.length && cols(recipeIdx).nonEmpty)
            cols(recipeIdx) = recipeMapping(cols(recipeIdx))
          cols.mkString(",")
        }
      }

      val pw = new PrintWriter(file, "UTF-8")
      try rewritten.foreach(pw.println)
      finally pw.close()

      println(s"Rewrote ${file.getName}  (${lines.size - 1} data rows)")
    }
  }

  // ── Print mapping tables for reference ─────────────────────────────────────
  println(s"\n${"=" * 70}")
  println(s"Cluster name mappings  (${clusterMapping.size} unique values)")
  println("=" * 70)
  clusterMapping.toSeq.sortBy(_._2).foreach { case (orig, anon) =>
    println(f"  $anon%-30s  ←  $orig")
  }

  println(s"\n${"=" * 70}")
  println(s"Recipe filename mappings  (${recipeMapping.size} unique values)")
  println("=" * 70)
  recipeMapping.toSeq.sortBy(_._2).foreach { case (orig, anon) =>
    println(f"  $anon%-45s  ←  $orig")
  }
}
