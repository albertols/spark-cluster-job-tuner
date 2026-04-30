package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.Json

/**
 * Raw-string JSON surgery for marking preserve_historical recipes inside a tuned
 * cluster JSON. Two operations:
 *
 *   - tagPreservedRecipes(json, names, refDate)
 *       Adds `lastTunedDate` and `keptWithoutCurrentDate: true` keys to the named
 *       recipes inside `recipeSparkConf`. If a recipe already carries a
 *       `lastTunedDate`, that value is preserved (recursive carry across runs).
 *
 *   - mergeRecipeBlocks(json, blocks)
 *       Inserts each `(recipeName -> recipeBlockJson)` into `recipeSparkConf`.
 *       Recipes that already exist are NOT overwritten.
 *
 *   - extractRecipeBlock(json, recipeName)
 *       Returns the raw `{ ... }` JSON object string for a recipe inside
 *       `recipeSparkConf`, or None if absent.
 *
 * The helpers operate on the raw JSON text because the tuner's existing JSON
 * builder (`Json`) and parser (`SimpleJsonParser`) are intentionally minimal and
 * do not provide a typed AST. Output is always re-prettified.
 */
private[auto] object KeptRecipeCarrier {

  /** Tag the named recipes in `json`'s `recipeSparkConf` block. Pretty-prints. */
  def tagPreservedRecipes(json: String, recipes: Set[String], fallbackRefDate: String): String = {
    if (recipes.isEmpty) return Json.pretty(json)
    val confStart = locateBlockOpenBrace(json, "recipeSparkConf")
    if (confStart < 0) return Json.pretty(json)
    val confEnd = findMatchingClose(json, confStart)
    if (confEnd < 0) return Json.pretty(json)

    var working = json
    // Re-locate the recipeSparkConf block on each iteration because string
    // surgery shifts indices.
    recipes.foreach { name =>
      working = tagOneRecipe(working, name, fallbackRefDate)
    }
    Json.pretty(working)
  }

  /** Insert recipe blocks into the recipeSparkConf object. Skips names that already exist. */
  def mergeRecipeBlocks(json: String, blocks: Seq[(String, String)]): String = {
    if (blocks.isEmpty) return Json.pretty(json)
    val confStart = locateBlockOpenBrace(json, "recipeSparkConf")
    if (confStart < 0) return Json.pretty(json)
    val confEnd = findMatchingClose(json, confStart)
    if (confEnd < 0) return Json.pretty(json)

    val existingKeys: Set[String] = topLevelKeys(json.substring(confStart, confEnd + 1)).toSet
    val toAdd = blocks.filterNot { case (k, _) => existingKeys.contains(k) }
    if (toAdd.isEmpty) return Json.pretty(json)

    val before = json.substring(0, confEnd)
    val after = json.substring(confEnd)
    val innerContent = json.substring(confStart + 1, confEnd).trim
    val sep = if (innerContent.isEmpty) "" else ","
    val addition = toAdd.map { case (k, v) => s""""${escapeKey(k)}": $v""" }.mkString(",")
    Json.pretty(before + sep + addition + after)
  }

  /** Extract the raw `{ ... }` block for a recipe inside recipeSparkConf. */
  def extractRecipeBlock(json: String, recipeName: String): Option[String] = {
    val confStart = locateBlockOpenBrace(json, "recipeSparkConf")
    if (confStart < 0) return None
    val confEnd = findMatchingClose(json, confStart)
    if (confEnd < 0) return None

    val confBlock = json.substring(confStart, confEnd + 1)
    val recipeBraceIdx = locateBlockOpenBrace(confBlock, recipeName)
    if (recipeBraceIdx < 0) return None
    val recipeEnd = findMatchingClose(confBlock, recipeBraceIdx)
    if (recipeEnd < 0) return None
    Some(confBlock.substring(recipeBraceIdx, recipeEnd + 1))
  }

  // ── Internal helpers ─────────────────────────────────────────────────────

  private def tagOneRecipe(json: String, recipeName: String, fallbackRefDate: String): String = {
    val confStart = locateBlockOpenBrace(json, "recipeSparkConf")
    if (confStart < 0) return json
    val confEnd = findMatchingClose(json, confStart)
    if (confEnd < 0) return json

    val confBlock = json.substring(confStart, confEnd + 1)
    val relativeBraceIdx = locateBlockOpenBrace(confBlock, recipeName)
    if (relativeBraceIdx < 0) return json
    val braceIdx = confStart + relativeBraceIdx
    val recipeEnd = findMatchingClose(json, braceIdx)
    if (recipeEnd < 0) return json

    val inner = json.substring(braceIdx + 1, recipeEnd)
    val existingDate = """"lastTunedDate"\s*:\s*"([^"]+)"""".r.findFirstMatchIn(inner).map(_.group(1))
    val resolvedDate = existingDate.getOrElse(fallbackRefDate)

    // Strip pre-existing flag pairs (with optional trailing/leading commas) to avoid duplicates.
    val cleanedInner = inner
      .replaceAll(""",\s*"lastTunedDate"\s*:\s*"[^"]*"""", "")
      .replaceAll(""""lastTunedDate"\s*:\s*"[^"]*"\s*,?""", "")
      .replaceAll(""",\s*"keptWithoutCurrentDate"\s*:\s*(?:true|false)""", "")
      .replaceAll(""""keptWithoutCurrentDate"\s*:\s*(?:true|false)\s*,?""", "")
      .trim

    val flags = s""""lastTunedDate": "$resolvedDate","keptWithoutCurrentDate": true"""
    val newInner = if (cleanedInner.isEmpty) flags else s"$flags,$cleanedInner"
    json.substring(0, braceIdx + 1) + newInner + json.substring(recipeEnd)
  }

  /** Find the position of the opening `{` for a top-level key. */
  private def locateBlockOpenBrace(json: String, key: String): Int = {
    val pat = s""""${java.util.regex.Pattern.quote(key)}"\\s*:\\s*\\{""".r
    pat.findFirstMatchIn(json).map(_.end - 1).getOrElse(-1)
  }

  /** Given the index of an opening `{`, find the matching `}` (string-aware). */
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

  /** Top-level keys of an object string (must include the outer braces). */
  private def topLevelKeys(json: String): Seq[String] = {
    val keys = scala.collection.mutable.ArrayBuffer.empty[String]
    var depth = 0
    var inString = false
    var escaped = false
    var i = 0
    while (i < json.length) {
      val c = json.charAt(i)
      if (escaped) {
        escaped = false
      } else if (inString) {
        if (c == '\\') escaped = true
        else if (c == '"') inString = false
      } else {
        c match {
          case '{' | '[' => depth += 1
          case '}' | ']' => depth -= 1
          case '"' if depth == 1 =>
            val end = json.indexOf('"', i + 1)
            if (end > i) {
              val candidate = json.substring(i + 1, end)
              val tail = json.substring(end + 1).dropWhile(_.isWhitespace)
              if (tail.startsWith(":")) keys += candidate
              i = end
            }
          case _ =>
        }
      }
      i += 1
    }
    keys.toSeq
  }

  private def escapeKey(s: String): String =
    s.flatMap {
      case '"' => "\\\""
      case '\\' => "\\\\"
      case c => c.toString
    }
}