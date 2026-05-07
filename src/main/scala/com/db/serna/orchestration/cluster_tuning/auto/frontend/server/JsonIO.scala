package com.db.serna.orchestration.cluster_tuning.auto.frontend.server

import scala.collection.mutable

/**
 * Tiny JSON read/write tailored to the Tuner Service's needs.
 *
 * Why hand-rolled: avoids a new runtime dep (jackson, circe). The shapes we serialise are flat and small (config
 * blocks, run requests, run status, dir listings) — full-featured JSON libs would be overkill.
 *
 * Parsed values use these types:
 *   - null
 *   - java.lang.Boolean
 *   - java.lang.Long (integers)
 *   - java.lang.Double (anything with `.`, `e`, or `E`)
 *   - String
 *   - mutable.LinkedHashMap[String, Any]
 *   - mutable.ArrayBuffer[Any]
 *
 * Writers accept Scala primitives + Map/Seq plus the parsed types above.
 */
object JsonIO {

  // ── Public API ────────────────────────────────────────────────────────────

  def parse(text: String): Any = {
    val p = new Parser(text)
    p.skipWs()
    val out = p.readValue()
    p.skipWs()
    if (!p.atEnd) throw new ParseException(s"Trailing junk at offset ${p.pos}")
    out
  }

  def parseObject(text: String): mutable.LinkedHashMap[String, Any] = parse(text) match {
    case m: mutable.LinkedHashMap[_, _] => m.asInstanceOf[mutable.LinkedHashMap[String, Any]]
    case other => throw new ParseException(s"Expected object at top level, got ${other.getClass.getSimpleName}")
  }

  def stringify(value: Any, pretty: Boolean = false): String = {
    val sb = new java.lang.StringBuilder
    write(sb, value, if (pretty) 0 else -1)
    sb.toString
  }

  /** Fetches a string field; returns the default if absent or wrong type. */
  def str(m: collection.Map[String, Any], key: String, default: String = ""): String =
    m.get(key) match {
      case Some(s: String) => s
      case _ => default
    }

  def boolOpt(m: collection.Map[String, Any], key: String): Option[Boolean] =
    m.get(key).flatMap {
      case b: java.lang.Boolean => Some(b.booleanValue())
      case s: String if s.equalsIgnoreCase("true") => Some(true)
      case s: String if s.equalsIgnoreCase("false") => Some(false)
      case _ => None
    }

  def numOpt(m: collection.Map[String, Any], key: String): Option[Double] =
    m.get(key).flatMap {
      case n: java.lang.Long => Some(n.doubleValue())
      case n: java.lang.Double => Some(n.doubleValue())
      case n: java.lang.Integer => Some(n.doubleValue())
      case s: String =>
        try Some(s.toDouble)
        catch { case _: NumberFormatException => None }
      case _ => None
    }

  def strOpt(m: collection.Map[String, Any], key: String): Option[String] =
    m.get(key).collect { case s: String => s }

  // ── Writer ────────────────────────────────────────────────────────────────

  private def write(sb: java.lang.StringBuilder, v: Any, indent: Int): Unit = v match {
    case null => sb.append("null")
    case b: Boolean => sb.append(if (b) "true" else "false")
    case b: java.lang.Boolean => sb.append(if (b.booleanValue()) "true" else "false")
    case n: Int => sb.append(n)
    case n: Long => sb.append(n)
    case n: java.lang.Long => sb.append(n.toString)
    case n: java.lang.Integer => sb.append(n.toString)
    case n: Float => writeNumber(sb, n.toDouble)
    case n: Double => writeNumber(sb, n)
    case n: java.lang.Double => writeNumber(sb, n.doubleValue())
    case s: String => writeString(sb, s)
    case None => sb.append("null")
    case Some(x) => write(sb, x, indent)
    case it: Iterable[_] =>
      it match {
        case m: collection.Map[_, _] =>
          val nextIndent = if (indent < 0) -1 else indent + 1
          sb.append('{')
          var first = true
          m.foreach { case (k, vv) =>
            if (!first) sb.append(',')
            if (indent >= 0) { sb.append('\n'); appendIndent(sb, nextIndent) }
            writeString(sb, String.valueOf(k))
            sb.append(if (indent >= 0) ": " else ":")
            write(sb, vv, nextIndent)
            first = false
          }
          if (indent >= 0 && !first) { sb.append('\n'); appendIndent(sb, indent) }
          sb.append('}')
        case _ =>
          val nextIndent = if (indent < 0) -1 else indent + 1
          sb.append('[')
          var first = true
          it.foreach { vv =>
            if (!first) sb.append(',')
            if (indent >= 0) { sb.append('\n'); appendIndent(sb, nextIndent) }
            write(sb, vv, nextIndent)
            first = false
          }
          if (indent >= 0 && !first) { sb.append('\n'); appendIndent(sb, indent) }
          sb.append(']')
      }
    case other => writeString(sb, String.valueOf(other))
  }

  private def writeNumber(sb: java.lang.StringBuilder, d: Double): Unit = {
    if (d.isNaN || d.isInfinite) sb.append("null")
    else if (d == d.toLong.toDouble && Math.abs(d) < 1e15) sb.append(d.toLong.toString)
    else sb.append(d.toString)
  }

  private def writeString(sb: java.lang.StringBuilder, s: String): Unit = {
    sb.append('"')
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case ch if ch < 0x20 => sb.append("\\u%04x".format(ch.toInt))
        case ch => sb.append(ch)
      }
      i += 1
    }
    sb.append('"')
  }

  private def appendIndent(sb: java.lang.StringBuilder, level: Int): Unit = {
    var i = 0
    while (i < level) { sb.append("  "); i += 1 }
  }

  // ── Parser ────────────────────────────────────────────────────────────────

  class ParseException(msg: String) extends RuntimeException(msg)

  private final class Parser(src: String) {
    var pos: Int = 0
    private val n = src.length

    def atEnd: Boolean = pos >= n

    def skipWs(): Unit = {
      while (
        pos < n && (src.charAt(pos) match {
          case ' ' | '\t' | '\n' | '\r' => true
          case _ => false
        })
      ) pos += 1
    }

    def readValue(): Any = {
      skipWs()
      if (atEnd) throw new ParseException("Unexpected EOF")
      src.charAt(pos) match {
        case '{' => readObject()
        case '[' => readArray()
        case '"' => readString()
        case 't' | 'f' => readBool()
        case 'n' => readNull()
        case c if c == '-' || (c >= '0' && c <= '9') => readNumber()
        case other => throw new ParseException(s"Unexpected '$other' at offset $pos")
      }
    }

    private def readObject(): mutable.LinkedHashMap[String, Any] = {
      pos += 1 // consume '{'
      val out = mutable.LinkedHashMap.empty[String, Any]
      skipWs()
      if (pos < n && src.charAt(pos) == '}') { pos += 1; return out }
      while (true) {
        skipWs()
        if (pos >= n || src.charAt(pos) != '"') throw new ParseException(s"Expected string key at offset $pos")
        val key = readString()
        skipWs()
        if (pos >= n || src.charAt(pos) != ':') throw new ParseException(s"Expected ':' at offset $pos")
        pos += 1
        out.put(key, readValue())
        skipWs()
        if (pos >= n) throw new ParseException("Unterminated object")
        src.charAt(pos) match {
          case ',' => pos += 1
          case '}' => pos += 1; return out
          case other => throw new ParseException(s"Expected ',' or '}' at offset $pos, got '$other'")
        }
      }
      out
    }

    private def readArray(): mutable.ArrayBuffer[Any] = {
      pos += 1 // consume '['
      val out = mutable.ArrayBuffer.empty[Any]
      skipWs()
      if (pos < n && src.charAt(pos) == ']') { pos += 1; return out }
      while (true) {
        out += readValue()
        skipWs()
        if (pos >= n) throw new ParseException("Unterminated array")
        src.charAt(pos) match {
          case ',' => pos += 1
          case ']' => pos += 1; return out
          case other => throw new ParseException(s"Expected ',' or ']' at offset $pos, got '$other'")
        }
      }
      out
    }

    private def readString(): String = {
      if (src.charAt(pos) != '"') throw new ParseException(s"Expected double-quote at offset $pos")
      pos += 1
      val sb = new java.lang.StringBuilder
      while (pos < n) {
        val c = src.charAt(pos)
        if (c == '"') { pos += 1; return sb.toString }
        if (c == '\\') {
          if (pos + 1 >= n) throw new ParseException("Unterminated escape")
          val esc = src.charAt(pos + 1)
          esc match {
            case '"' => sb.append('"'); pos += 2
            case '\\' => sb.append('\\'); pos += 2
            case '/' => sb.append('/'); pos += 2
            case 'n' => sb.append('\n'); pos += 2
            case 'r' => sb.append('\r'); pos += 2
            case 't' => sb.append('\t'); pos += 2
            case 'b' => sb.append('\b'); pos += 2
            case 'f' => sb.append('\f'); pos += 2
            case 'u' =>
              if (pos + 5 >= n) throw new ParseException("Truncated \\uXXXX")
              val hex = src.substring(pos + 2, pos + 6)
              sb.append(Integer.parseInt(hex, 16).toChar)
              pos += 6
            case other => throw new ParseException(s"Bad escape \\$other at offset $pos")
          }
        } else {
          sb.append(c); pos += 1
        }
      }
      throw new ParseException("Unterminated string")
    }

    private def readBool(): java.lang.Boolean = {
      if (src.startsWith("true", pos)) { pos += 4; java.lang.Boolean.TRUE }
      else if (src.startsWith("false", pos)) { pos += 5; java.lang.Boolean.FALSE }
      else throw new ParseException(s"Expected bool at offset $pos")
    }

    private def readNull(): AnyRef = {
      if (src.startsWith("null", pos)) { pos += 4; null }
      else throw new ParseException(s"Expected null at offset $pos")
    }

    private def readNumber(): AnyRef = {
      val start = pos
      if (src.charAt(pos) == '-') pos += 1
      while (pos < n && src.charAt(pos) >= '0' && src.charAt(pos) <= '9') pos += 1
      var isFloat = false
      if (pos < n && src.charAt(pos) == '.') {
        isFloat = true; pos += 1
        while (pos < n && src.charAt(pos) >= '0' && src.charAt(pos) <= '9') pos += 1
      }
      if (pos < n && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
        isFloat = true; pos += 1
        if (pos < n && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) pos += 1
        while (pos < n && src.charAt(pos) >= '0' && src.charAt(pos) <= '9') pos += 1
      }
      val raw = src.substring(start, pos)
      if (isFloat) java.lang.Double.valueOf(raw)
      else java.lang.Long.valueOf(raw)
    }
  }
}
