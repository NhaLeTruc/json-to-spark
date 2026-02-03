package com.pipeline.metrics

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Metrics collector for pipeline execution.
 *
 * Tracks execution times, record counts, and bytes processed
 * for monitoring and performance analysis.
 */
case class PipelineMetrics(
    pipelineName: String,
    mode: String,
    startTime: Long = System.currentTimeMillis(),
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  @volatile var endTime: Option[Long]        = None
  @volatile var status: String               = "RUNNING"
  @volatile var errorMessage: Option[String] = None

  // Step-level metrics
  private val stepMetrics = mutable.ListBuffer[StepMetrics]()

  // Overall metrics
  @volatile var totalRecordsRead: Long    = 0
  @volatile var totalRecordsWritten: Long = 0
  @volatile var totalBytesRead: Long      = 0
  @volatile var totalBytesWritten: Long   = 0

  /**
   * Records the start of a pipeline step.
   */
  def startStep(stepIndex: Int, stepType: String, method: String): Unit = {
    logger.debug(s"Starting step $stepIndex: $stepType.$method")
    stepMetrics += StepMetrics(
      stepIndex = stepIndex,
      stepType = stepType,
      method = method,
      startTime = System.currentTimeMillis(),
    )
  }

  /**
   * Records the completion of a pipeline step.
   */
  def endStep(
      stepIndex: Int,
      recordsRead: Long = 0,
      recordsWritten: Long = 0,
      bytesRead: Long = 0,
      bytesWritten: Long = 0,
  ): Unit =
    stepMetrics.find(_.stepIndex == stepIndex) match {
      case Some(metrics) =>
        metrics.endTime = Some(System.currentTimeMillis())
        metrics.status = "COMPLETED"
        metrics.recordsRead = recordsRead
        metrics.recordsWritten = recordsWritten
        metrics.bytesRead = bytesRead
        metrics.bytesWritten = bytesWritten

        // Update totals
        totalRecordsRead += recordsRead
        totalRecordsWritten += recordsWritten
        totalBytesRead += bytesRead
        totalBytesWritten += bytesWritten

        logger.debug(s"Completed step $stepIndex: ${metrics.durationMs}ms, $recordsRead records")

      case None =>
        logger.warn(s"No start metrics found for step $stepIndex")
    }

  /**
   * Records a step failure.
   */
  def failStep(stepIndex: Int, error: String): Unit =
    stepMetrics.find(_.stepIndex == stepIndex) match {
      case Some(metrics) =>
        metrics.endTime = Some(System.currentTimeMillis())
        metrics.status = "FAILED"
        metrics.errorMessage = Some(error)
        logger.debug(s"Failed step $stepIndex: $error")

      case None =>
        logger.warn(s"No start metrics found for failed step $stepIndex")
    }

  /**
   * Marks the pipeline as completed successfully.
   */
  def complete(): Unit = {
    endTime = Some(System.currentTimeMillis())
    status = "COMPLETED"
    logger.info(
      s"Pipeline completed: $pipelineName, duration=${durationMs}ms, records=$totalRecordsRead→$totalRecordsWritten",
    )
  }

  /**
   * Marks the pipeline as failed.
   */
  def fail(error: String): Unit = {
    endTime = Some(System.currentTimeMillis())
    status = "FAILED"
    errorMessage = Some(error)
    logger.warn(s"Pipeline failed: $pipelineName, duration=${durationMs}ms, error=$error")
  }

  /**
   * Marks the pipeline as cancelled.
   */
  def cancel(): Unit = {
    endTime = Some(System.currentTimeMillis())
    status = "CANCELLED"
    logger.info(s"Pipeline cancelled: $pipelineName, duration=${durationMs}ms")
  }

  /**
   * Gets the total duration in milliseconds.
   */
  def durationMs: Long =
    endTime.getOrElse(System.currentTimeMillis()) - startTime

  /**
   * Gets all step metrics.
   */
  def getStepMetrics: List[StepMetrics] = stepMetrics.toList

  /**
   * Converts metrics to a map for serialization.
   */
  def toMap: Map[String, Any] =
    Map(
      "pipelineName"        -> pipelineName,
      "mode"                -> mode,
      "status"              -> status,
      "startTime"           -> startTime,
      "endTime"             -> endTime.getOrElse(System.currentTimeMillis()),
      "durationMs"          -> durationMs,
      "totalRecordsRead"    -> totalRecordsRead,
      "totalRecordsWritten" -> totalRecordsWritten,
      "totalBytesRead"      -> totalBytesRead,
      "totalBytesWritten"   -> totalBytesWritten,
      "errorMessage"        -> errorMessage.getOrElse(""),
      "steps"               -> stepMetrics.map(_.toMap).toList,
    )

  /**
   * Converts metrics to JSON string.
   */
  def toJson: String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    Serialization.write(toMap)
  }
}

/**
 * Metrics for a single pipeline step.
 */
case class StepMetrics(
    stepIndex: Int,
    stepType: String,
    method: String,
    startTime: Long,
) {

  @volatile var endTime: Option[Long]        = None
  @volatile var status: String               = "RUNNING"
  @volatile var errorMessage: Option[String] = None

  @volatile var recordsRead: Long    = 0
  @volatile var recordsWritten: Long = 0
  @volatile var bytesRead: Long      = 0
  @volatile var bytesWritten: Long   = 0

  /**
   * Gets the step duration in milliseconds.
   */
  def durationMs: Long =
    endTime.getOrElse(System.currentTimeMillis()) - startTime

  /**
   * Converts step metrics to a map.
   */
  def toMap: Map[String, Any] =
    Map(
      "stepIndex"      -> stepIndex,
      "stepType"       -> stepType,
      "method"         -> method,
      "status"         -> status,
      "startTime"      -> startTime,
      "endTime"        -> endTime.getOrElse(System.currentTimeMillis()),
      "durationMs"     -> durationMs,
      "recordsRead"    -> recordsRead,
      "recordsWritten" -> recordsWritten,
      "bytesRead"      -> bytesRead,
      "bytesWritten"   -> bytesWritten,
      "errorMessage"   -> errorMessage.getOrElse(""),
    )
}

/**
 * Factory for creating PipelineMetrics.
 */
object PipelineMetrics {

  /**
   * Creates metrics for a pipeline.
   */
  def apply(pipelineName: String, mode: String): PipelineMetrics =
    new PipelineMetrics(pipelineName, mode)
}
