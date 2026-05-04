package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.Json
import com.db.serna.orchestration.cluster_tuning.single.refinement.SimpleJsonParser

/**
 * Re-injects recipe-level boost metadata from a reference output JSON into a
 * freshly-emitted current JSON. Companion to [[KeptRecipeCarrier]].
 *
 * When the AutoTuner re-plans a cluster on the `BoostResources` /
 * `GenerateFresh` path, the new recipe blocks are built from current-date
 * metrics with no awareness of prior boosts. Without this carry step, the
 * downstream b16 reboosting cannot see the prior `appliedMemoryHeapBoostFactor`
 * tag and would either treat the recipe as baseline (when the b16 CSV no
 * longer reports the recipe) or only apply a fresh `New` boost without
 * compounding the cumulative factor.
 *
 * Per recipe, the carry copies:
 *   - `appliedMemoryHeapBoostFactor`        (recipe-level cumulative factor)
 *   - `spark.executor.memory`               (boosted GB string, e.g. "12g")
 *   - `total_executor_minimum_allocated_memory_gb` re-derived from the cur
 *     recipe's executor counts × boosted memGb
 *   - `total_executor_maximum_allocated_memory_gb` (same)
 *
 * Pure raw-JSON surgery — does not round-trip through SimpleJsonParser, so
 * fields that the lightweight parser does not understand (e.g. nested
 * `cost_timeline` arrays) are left untouched.
 */
private[auto] object BoostMetadataCarrier {

  private val factorRe = """"appliedMemoryHeapBoostFactor"\s*:\s*([\d.]+)""".r
  private val memRe = """"spark\.executor\.memory"\s*:\s*"([^"]+)"""".r
  private val isDynamicRe = """"spark\.dynamicAllocation\.enabled"\s*:\s*"true"""".r
  private val minExecRe = """"spark\.dynamicAllocation\.minExecutors"\s*:\s*"?(\d+)"?""".r
  private val maxExecRe = """"spark\.dynamicAllocation\.maxExecutors"\s*:\s*"?(\d+)"?""".r
  private val instancesRe = """"spark\.executor\.instances"\s*:\s*"?(\d+)"?""".r

  /**
   * Inject prior b16 boost metadata for each recipe in `recipeNames`. Recipes
   * whose reference block has no `appliedMemoryHeapBoostFactor` are skipped,
   * as are recipes missing from the current JSON. Returns the updated JSON
   * (pretty-printed) — equal to the input when nothing was carried.
   */
  def injectPriorBoosts(curJson: String, refJson: String, recipeNames: Set[String]): String = {
    if (recipeNames.isEmpty) return curJson
    var working = curJson
    var carried = 0
    recipeNames.foreach { name =>
      injectOne(working, refJson, name) match {
        case Some(updated) =>
          working = updated
          carried += 1
        case None =>
      }
    }
    if (carried == 0) curJson else Json.pretty(working)
  }

  private def injectOne(curJson: String, refJson: String, recipeName: String): Option[String] = {
    for {
      refBlock <- KeptRecipeCarrier.extractRecipeBlock(refJson, recipeName)
      priorFactor <- factorRe.findFirstMatchIn(refBlock).map(_.group(1))
      boostedMem <- memRe.findFirstMatchIn(refBlock).map(_.group(1))
      curBlock <- KeptRecipeCarrier.extractRecipeBlock(curJson, recipeName)
    } yield {
      val (minE, maxE) = extractExecutorCounts(curBlock)
      val memGb = SimpleJsonParser.parseMemoryGb(boostedMem)
      val newMinTotal = minE * memGb
      val newMaxTotal = maxE * memGb

      val patchedBlock = patchBlock(curBlock, priorFactor, boostedMem, newMinTotal, newMaxTotal)
      replaceRecipeBlock(curJson, recipeName, patchedBlock)
    }
  }

  private def extractExecutorCounts(curBlock: String): (Int, Int) = {
    if (isDynamicRe.findFirstIn(curBlock).isDefined) {
      val min = minExecRe.findFirstMatchIn(curBlock).map(_.group(1).toInt).getOrElse(1)
      val max = maxExecRe.findFirstMatchIn(curBlock).map(_.group(1).toInt).getOrElse(min)
      (min, max)
    } else {
      val inst = instancesRe.findFirstMatchIn(curBlock).map(_.group(1).toInt).getOrElse(1)
      (inst, inst)
    }
  }

  private def patchBlock(
                          curBlock: String,
                          priorFactor: String,
                          boostedMem: String,
                          newMinTotal: Int,
                          newMaxTotal: Int
                        ): String = {
    val memReplacement = "$1" + java.util.regex.Matcher.quoteReplacement(boostedMem) + "$2"
    var b = curBlock
      .replaceAll(
        """("spark\.executor\.memory"\s*:\s*")[^"]+(")""",
        memReplacement
      )
      .replaceAll(
        """("total_executor_minimum_allocated_memory_gb"\s*:\s*)\d+""",
        "$1" + newMinTotal
      )
      .replaceAll(
        """("total_executor_maximum_allocated_memory_gb"\s*:\s*)\d+""",
        "$1" + newMaxTotal
      )

    if (b.contains("\"appliedMemoryHeapBoostFactor\"")) {
      b = b.replaceAll(
        """("appliedMemoryHeapBoostFactor"\s*:\s*)[\d.]+""",
        "$1" + priorFactor
      )
    } else {
      // Insert immediately after parallelizationFactor (matches the layout
      // produced by RefinementPipeline.toRefinedJson).
      val factorInsertion = "$1," + java.util.regex.Matcher.quoteReplacement(s""""appliedMemoryHeapBoostFactor": $priorFactor,""")
      b = b.replaceFirst(
        """("parallelizationFactor"\s*:\s*\d+)\s*,""",
        factorInsertion
      )
    }
    b
  }

  /**
   * Replace the recipe's `{ ... }` block within `curJson`. Anchors on the
   * recipe key (`"<recipeName>"`) FIRST and then locates its `{...}` payload
   * via brace-balance walking. Naïve `indexOf(oldBlock)` is unsafe when two
   * sibling recipes happen to share an identical inner block (which is the
   * common case for baseline-planned recipes that haven't been boosted yet).
   */
  private def replaceRecipeBlock(curJson: String, recipeName: String, newBlock: String): String = {
    val keyPattern = ("\"" + java.util.regex.Pattern.quote(recipeName) + "\"\\s*:\\s*\\{").r
    keyPattern.findFirstMatchIn(curJson) match {
      case Some(m) =>
        val openIdx = m.end - 1 // position of the `{`
        val closeIdx = findMatchingClose(curJson, openIdx)
        if (closeIdx < 0) curJson
        else curJson.substring(0, openIdx) + newBlock + curJson.substring(closeIdx + 1)
      case None => curJson
    }
  }

  /** Walk forward from `openBracePos` (a `{`) to its matching `}`, string-aware. */
  private def findMatchingClose(json: String, openBracePos: Int): Int = {
    var depth = 0
    var inString = false
    var escaped = false
    var i = openBracePos
    while (i < json.length) {
      val c = json.charAt(i)
      if (escaped) {
        escaped = false
      } else if (inString) {
        if (c == '\\') escaped = true
        else if (c == '"') inString = false
      } else {
        c match {
          case '"' => inString = true
          case '{' => depth += 1
          case '}' =>
            depth -= 1
            if (depth == 0) return i
          case _ =>
        }
      }
      i += 1
    }
    -1
  }
}
