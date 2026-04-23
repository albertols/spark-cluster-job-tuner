package com.db.serna.orchestration.cluster_tuning.single.refinement

import java.io.File
import scala.io.Source
import scala.util.Try

/** Parsed representation of a tuned cluster JSON file (auto-scale or manual). */
final case class TunedClusterConfig(
  rawJson: String,
  clusterName: String,
  clusterConfFields: Seq[(String, String)],
  recipeOrder: Seq[String],
  recipes: Map[String, RecipeConfig]
)

/** Per-recipe Spark configuration extracted from a tuned JSON. */
final case class RecipeConfig(
  parallelizationFactor: Int,
  sparkOptsMap: Map[String, String],
  totalExecutorMinAllocatedMemoryGb: Int,
  totalExecutorMaxAllocatedMemoryGb: Int,
  extraFields: Map[String, String]
)

/**
 * Lightweight JSON parser for the machine-generated tuned cluster JSONs.
 *
 * The JSON schema is fixed and well-known (produced by ClusterMachineAndRecipeTuner),
 * so we use targeted regex extraction rather than pulling in a full JSON library.
 */
object SimpleJsonParser {

  def parseFile(file: File): TunedClusterConfig = {
    val src = Source.fromFile(file)
    val raw = try src.mkString finally src.close()
    parse(raw)
  }

  def parse(raw: String): TunedClusterConfig = {
    val clusterConfBlock = extractBlock(raw, "clusterConf")
    val recipeSparkConfBlock = extractBlock(raw, "recipeSparkConf")

    val clusterName = extractFirstKey(clusterConfBlock)
    val clusterInnerBlock = extractBlock(clusterConfBlock, clusterName)
    val clusterConfFields = extractOrderedFlatFields(clusterInnerBlock)

    val (recipeOrder, recipes) = extractRecipes(recipeSparkConfBlock)

    TunedClusterConfig(raw, clusterName, clusterConfFields, recipeOrder, recipes)
  }

  private[refinement] def extractRecipes(recipeBlock: String): (Seq[String], Map[String, RecipeConfig]) = {
    val recipeKeys = extractAllKeys(recipeBlock)
    val entries = recipeKeys.flatMap { key =>
      Try {
        val block = extractBlock(recipeBlock, key)
        val pf = extractIntField(block, "parallelizationFactor").getOrElse(5)
        val sparkOptsBlock = extractBlock(block, "sparkOptsMap")
        val sparkOpts = extractStringFields(sparkOptsBlock)
        val minMem = extractIntField(block, "total_executor_minimum_allocated_memory_gb").getOrElse(0)
        val maxMem = extractIntField(block, "total_executor_maximum_allocated_memory_gb").getOrElse(0)
        key -> RecipeConfig(pf, sparkOpts, minMem, maxMem, Map.empty)
      }.toOption
    }
    (recipeKeys, entries.toMap)
  }

  /** Extract a JSON object block by its key name. Returns the content between the outer braces. */
  private[refinement] def extractBlock(json: String, key: String): String = {
    val escapedKey = java.util.regex.Pattern.quote(key)
    val keyPattern = s""""$escapedKey"\\s*:\\s*\\{""".r
    keyPattern.findFirstMatchIn(json) match {
      case Some(m) =>
        val start = m.end - 1
        findMatchingBrace(json, start)
      case None => ""
    }
  }

  private def findMatchingBrace(json: String, openPos: Int): String = {
    var depth = 0
    var inString = false
    var i = openPos
    while (i < json.length) {
      val c = json.charAt(i)
      if (inString) {
        if (c == '\\') i += 1
        else if (c == '"') inString = false
      } else {
        c match {
          case '"' => inString = true
          case '{' => depth += 1
          case '}' =>
            depth -= 1
            if (depth == 0) return json.substring(openPos, i + 1)
          case _ =>
        }
      }
      i += 1
    }
    json.substring(openPos)
  }

  /** Extract the first key from a JSON object string. */
  private[refinement] def extractFirstKey(json: String): String = {
    val keyPattern = """"([^"]+)"\s*:""".r
    keyPattern.findFirstMatchIn(json).map(_.group(1)).getOrElse("")
  }

  /** Extract all top-level keys from a JSON object string. */
  private[refinement] def extractAllKeys(json: String): Seq[String] = {
    // We need to find keys at the top level of this object only
    val keys = scala.collection.mutable.ArrayBuffer.empty[String]
    var depth = 0
    var inString = false
    var i = 0
    while (i < json.length) {
      val c = json.charAt(i)
      if (inString) {
        if (c == '\\') i += 1
        else if (c == '"') inString = false
      } else {
        c match {
          case '{' | '[' => depth += 1
          case '}' | ']' => depth -= 1
          case '"' if depth == 1 =>
            // Potential key at top level — find the closing quote
            val end = json.indexOf('"', i + 1)
            if (end > i) {
              val candidate = json.substring(i + 1, end)
              // Check if followed by ':'
              val afterQuote = json.substring(end + 1).trim
              if (afterQuote.startsWith(":")) {
                keys += candidate
              }
              i = end
            }
          case _ =>
        }
      }
      i += 1
    }
    keys.toSeq
  }

  /** Extract flat string fields from a JSON object (key: "value" pairs). */
  private[refinement] def extractStringFields(json: String): Map[String, String] = {
    val pattern = """"([^"]+)"\s*:\s*"([^"]*)"""".r
    pattern.findAllMatchIn(json).map(m => m.group(1) -> m.group(2)).toMap
  }

  /** Extract flat fields preserving the original key order from the JSON. */
  private[refinement] def extractOrderedFlatFields(json: String): Seq[(String, String)] = {
    val keys = extractAllKeys(json)
    val stringPattern = """"([^"]+)"\s*:\s*"([^"]*)"""".r
    val numPattern = """"([^"]+)"\s*:\s*(\d+(?:\.\d+)?)""".r

    val stringFields = stringPattern.findAllMatchIn(json).map(m => m.group(1) -> m.group(2)).toMap
    val numFields = numPattern.findAllMatchIn(json)
      .map(m => m.group(1) -> m.group(2))
      .filterNot { case (k, _) => stringFields.contains(k) }
      .toMap

    val allFields = stringFields ++ numFields
    keys.flatMap(k => allFields.get(k).map(v => k -> v))
  }

  /** Extract an integer field value by name. */
  private[refinement] def extractIntField(json: String, fieldName: String): Option[Int] = {
    val escapedField = java.util.regex.Pattern.quote(fieldName)
    val pattern = s""""$escapedField"\\s*:\\s*(\\d+)""".r
    pattern.findFirstMatchIn(json).flatMap(m => Try(m.group(1).toInt).toOption)
  }

  /** Parse memory string like "8g" to integer GB value. */
  def parseMemoryGb(memStr: String): Int =
    Try(memStr.trim.toLowerCase.stripSuffix("g").toInt).getOrElse(0)
}
