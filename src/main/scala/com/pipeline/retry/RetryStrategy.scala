package com.pipeline.retry

import com.pipeline.exceptions.{PipelineException, RetryableException}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Retry strategy for pipeline execution.
 *
 * Implements FR-016: Retry failed pipelines up to 3 times with 5-second delays.
 * Uses tail recursion to avoid stack overflow.
 *
 * Validates Constitution Section VII: Idempotency and Fault Tolerance.
 */
object RetryStrategy {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes an operation with retry logic.
   *
   * @param operation    The operation to execute
   * @param maxAttempts  Maximum number of attempts (including initial)
   * @param delayMillis  Delay between retry attempts in milliseconds
   * @param attempt      Current attempt number (internal, starts at 1)
   * @tparam T Result type
   * @return Try containing the result or final failure
   */
  @tailrec
  def executeWithRetry[T](
      operation: () => Try[T],
      maxAttempts: Int = 3,
      delayMillis: Long = 5000,
      attempt: Int = 1,
  ): Try[T] = {

    logger.info(s"Executing operation (attempt $attempt of $maxAttempts)")

    val result = operation()

    result match {
      case Success(value) =>
        logger.info(s"Operation succeeded on attempt $attempt")
        Success(value)

      case Failure(exception) if attempt < maxAttempts =>
        logger.warn(
          s"Operation failed on attempt $attempt: ${exception.getMessage}. " +
            s"Retrying in ${delayMillis}ms...",
        )

        // Note: Thread.sleep is used for simplicity. For high-concurrency scenarios,
        // consider using ScheduledExecutorService or Scala Futures with delay.
        Thread.sleep(delayMillis)

        // Tail recursive call
        executeWithRetry(operation, maxAttempts, delayMillis, attempt + 1)

      case Failure(exception) =>
        logger.error(
          s"Operation failed after $maxAttempts attempts: ${exception.getMessage}",
          exception,
        )
        Failure(exception)
    }
  }

  /**
   * Wraps a pipeline execution function with retry logic.
   *
   * @param pipelineId   Identifier for the pipeline (for logging)
   * @param execution    The pipeline execution function
   * @param maxAttempts  Maximum retry attempts
   * @param delayMillis  Delay between retries
   * @tparam T Result type
   * @return Try containing the result or final failure
   */
  def withRetry[T](
      pipelineId: String,
      execution: => T,
      maxAttempts: Int = 3,
      delayMillis: Long = 5000,
  ): Try[T] = {
    org.slf4j.MDC.put("pipelineId", pipelineId)

    try executeWithRetry(() => Try(execution), maxAttempts, delayMillis)
    finally org.slf4j.MDC.remove("pipelineId")
  }

  /**
   * Executes an operation with smart retry logic based on exception type.
   *
   * Only retries if the exception is retryable (RetryableException or known transient errors).
   * Non-retryable exceptions fail immediately.
   *
   * @param operation    The operation to execute
   * @param maxAttempts  Maximum number of attempts (including initial)
   * @param delayMillis  Delay between retry attempts in milliseconds
   * @param attempt      Current attempt number (internal, starts at 1)
   * @tparam T Result type
   * @return Try containing the result or final failure
   */
  @tailrec
  def executeWithSmartRetry[T](
      operation: () => Try[T],
      maxAttempts: Int = 3,
      delayMillis: Long = 5000,
      attempt: Int = 1,
  ): Try[T] = {

    logger.info(s"Executing operation with smart retry (attempt $attempt of $maxAttempts)")

    val result = operation()

    result match {
      case Success(value) =>
        logger.info(s"Operation succeeded on attempt $attempt")
        Success(value)

      case Failure(exception) if attempt < maxAttempts && shouldRetry(exception) =>
        logger.warn(
          s"Operation failed with retryable error on attempt $attempt: ${exception.getMessage}. " +
            s"Retrying in ${delayMillis}ms...",
        )

        // Note: Thread.sleep is used for simplicity. For high-concurrency scenarios,
        // consider using ScheduledExecutorService or Scala Futures with delay.
        Thread.sleep(delayMillis)

        // Tail recursive call
        executeWithSmartRetry(operation, maxAttempts, delayMillis, attempt + 1)

      case Failure(exception) if !shouldRetry(exception) =>
        logger.error(
          s"Operation failed with non-retryable error: ${exception.getMessage}. Not retrying.",
          exception,
        )
        Failure(exception)

      case Failure(exception) =>
        logger.error(
          s"Operation failed after $maxAttempts attempts: ${exception.getMessage}",
          exception,
        )
        Failure(exception)
    }
  }

  /**
   * Determines if an exception should trigger a retry.
   *
   * @param ex The exception to check
   * @return true if the exception is retryable
   */
  private def shouldRetry(ex: Throwable): Boolean =
    ex match {
      case _: RetryableException => true
      case pe: PipelineException => PipelineException.isRetryable(pe)
      case _                     => PipelineException.isRetryable(ex)
    }

  /**
   * Wraps a pipeline execution function with smart retry logic.
   *
   * Only retries on transient/retryable errors.
   *
   * @param pipelineId   Identifier for the pipeline (for logging)
   * @param execution    The pipeline execution function
   * @param maxAttempts  Maximum retry attempts
   * @param delayMillis  Delay between retries
   * @tparam T Result type
   * @return Try containing the result or final failure
   */
  def withSmartRetry[T](
      pipelineId: String,
      execution: => T,
      maxAttempts: Int = 3,
      delayMillis: Long = 5000,
  ): Try[T] = {
    org.slf4j.MDC.put("pipelineId", pipelineId)

    try executeWithSmartRetry(() => Try(execution), maxAttempts, delayMillis)
    finally org.slf4j.MDC.remove("pipelineId")
  }
}
