package com.db.serna.orchestration.cluster_tuning.auto.frontend.server

import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.{Level, LogManager}

import java.io.Serializable
import scala.util.control.NonFatal

/**
 * log4j2 programmatic appender that streams every log event into the active run's [[RunRegistry.LogBuffer]].
 *
 * Lifecycle (managed by [[withRunCapture]]):
 *   - on entry: create + start an appender, attach to the root logger.
 *   - on exit (via finally): detach + stop. No mutation of stdout/stderr.
 *
 * The single-run gate in [[RunRegistry]] guarantees at most one capture at a time, so we don't need per-thread
 * isolation.
 */
final class RunLogAppender private (
    name: String,
    layout: PatternLayout,
    buffer: RunRegistry.LogBuffer
) extends AbstractAppender(name, /* filter */ null, layout, /* ignoreExceptions */ true, Property.EMPTY_ARRAY) {

  override def append(event: LogEvent): Unit = {
    try {
      val bytes = getLayout.toByteArray(event)
      val line = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).stripLineEnd
      if (line.nonEmpty) buffer.append(line)
    } catch {
      case NonFatal(_) => /* never let logging break the tuner */
    }
  }
}

object RunLogAppender {

  private val PATTERN = "%d{HH:mm:ss.SSS} %-5level %logger{1.} - %msg%n"

  /** Run `body` with all log events captured into `buffer`. */
  def withRunCapture[T](buffer: RunRegistry.LogBuffer)(body: => T): T = {
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val cfg = ctx.getConfiguration
    val layout = PatternLayout
      .newBuilder()
      .withConfiguration(cfg)
      .withPattern(PATTERN)
      .build()
    val name = "tuner-run-" + System.nanoTime()
    val appender = new RunLogAppender(name, layout, buffer)
    appender.start()

    val rootCfg = cfg.getRootLogger
    rootCfg.addAppender(appender, Level.INFO, /* filter */ null)
    ctx.updateLoggers()

    try body
    finally {
      try {
        rootCfg.removeAppender(name)
        ctx.updateLoggers()
      } catch { case NonFatal(_) => /* swallow */ }
      try appender.stop()
      catch { case NonFatal(_) => /* swallow */ }
    }
  }
}
