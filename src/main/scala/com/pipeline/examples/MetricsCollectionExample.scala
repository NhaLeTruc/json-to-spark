package com.pipeline.examples

import com.pipeline.config.PipelineConfigParser
import com.pipeline.core.Pipeline
import com.pipeline.metrics.{JsonFileExporter, LogExporter, PrometheusExporter}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * Example demonstrating pipeline metrics collection and export.
 *
 * Shows how to:
 * 1. Execute a pipeline with metrics collection enabled
 * 2. Export metrics to different formats (Prometheus, JSON, Logs)
 * 3. Access step-level and pipeline-level metrics
 */
object MetricsCollectionExample {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession
      .builder()
      .appName("MetricsCollectionExample")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    try {
      // Parse pipeline configuration
      val configPath = "config/examples/batch-with-metrics.json"
      logger.info(s"Loading pipeline configuration from: $configPath")

      val config   = PipelineConfigParser.parseFile(configPath)
      val pipeline = Pipeline.fromConfig(config)

      logger.info(s"Executing pipeline: ${config.name}")

      // Execute pipeline with metrics collection enabled
      val result = pipeline.execute(
        spark = spark,
        maxAttempts = 3,
        delayMillis = 5000,
        collectMetrics = true, // Enable metrics collection
      )

      result match {
        case Right(context) =>
          logger.info("Pipeline completed successfully")

          // Retrieve metrics
          pipeline.getMetrics match {
            case Some(metrics) =>
              logger.info("=" * 80)
              logger.info("METRICS COLLECTION DEMONSTRATION")
              logger.info("=" * 80)

              // 1. Export to logs (human-readable format)
              logger.info("\n1. EXPORTING TO LOGS (Human-Readable):")
              LogExporter.export(metrics)

              // 2. Export to logs with MDC context
              logger.info("\n2. EXPORTING WITH MDC CONTEXT:")
              LogExporter.exportWithMDC(metrics)

              // 3. Export to JSON (structured format)
              logger.info("\n3. EXPORTING TO JSON:")
              val tempDir = System.getProperty("java.io.tmpdir")
              val jsonOutput = s"$tempDir/pipeline-metrics.json"
              JsonFileExporter.export(metrics, jsonOutput, append = false)
              logger.info(s"Metrics exported to JSON: $jsonOutput")

              // 4. Export to JSON Lines (for aggregation)
              logger.info("\n4. EXPORTING TO JSONL (Append Mode):")
              val jsonlOutput = s"$tempDir/pipeline-metrics.jsonl"
              JsonFileExporter.exportToJsonLines(metrics, jsonlOutput)
              logger.info(s"Metrics appended to JSONL: $jsonlOutput")

              // 5. Export to Prometheus format
              logger.info("\n5. EXPORTING TO PROMETHEUS FORMAT:")
              val prometheusOutput = s"$tempDir/pipeline-metrics.prom"
              PrometheusExporter.exportToFile(metrics, prometheusOutput)
              logger.info(s"Metrics exported to Prometheus format: $prometheusOutput")

              // Display summary
              logger.info("\n" + "=" * 80)
              logger.info("METRICS SUMMARY")
              logger.info("=" * 80)
              logger.info(s"Pipeline Name: ${metrics.pipelineName}")
              logger.info(s"Mode: ${metrics.mode}")
              logger.info(s"Status: ${metrics.status}")
              logger.info(s"Duration: ${metrics.durationMs}ms")
              logger.info(s"Total Records Read: ${metrics.totalRecordsRead}")
              logger.info(s"Total Records Written: ${metrics.totalRecordsWritten}")
              logger.info(s"Total Bytes Read: ${metrics.totalBytesRead}")
              logger.info(s"Total Bytes Written: ${metrics.totalBytesWritten}")
              logger.info(s"Total Steps: ${metrics.getStepMetrics.size}")

              // Display step-by-step breakdown
              logger.info("\nSTEP-BY-STEP BREAKDOWN:")
              metrics.getStepMetrics.foreach { step =>
                logger.info(
                  s"  Step ${step.stepIndex}: ${step.stepType}.${step.method} - " +
                    s"${step.status} (${step.durationMs}ms)",
                )
              }

              logger.info("=" * 80)

            case None =>
              logger.warn("No metrics available (metrics collection was disabled)")
          }

        case Left(exception) =>
          logger.error("Pipeline execution failed", exception)

          // Even on failure, export metrics if available
          pipeline.getMetrics.foreach { metrics =>
            logger.info("Exporting failure metrics...")
            LogExporter.export(metrics)

            // Export to file for debugging
            val tempDir = System.getProperty("java.io.tmpdir")
            JsonFileExporter.export(
              metrics,
              s"$tempDir/pipeline-metrics-failed-${System.currentTimeMillis()}.json",
            )
          }
      }

    } finally {
      spark.stop()
      logger.info("SparkSession stopped")
    }
  }

  /**
   * Example showing programmatic metrics access.
   */
  def demonstrateMetricsAccess(spark: SparkSession): Unit = {
    val configPath = "config/examples/batch-postgres-to-s3.json"
    val config     = PipelineConfigParser.parseFile(configPath)
    val pipeline   = Pipeline.fromConfig(config)

    // Execute pipeline
    val result = pipeline.execute(spark, collectMetrics = true)

    result match {
      case Right(_) =>
        // Access metrics programmatically
        pipeline.getMetrics.foreach { metrics =>
          // Check pipeline duration
          if (metrics.durationMs > 60000) {
            logger.warn(
              s"Pipeline took longer than 1 minute: ${metrics.durationMs}ms",
            )
          }

          // Check for slow steps
          metrics.getStepMetrics.foreach { step =>
            if (step.durationMs > 10000) {
              logger.warn(
                s"Slow step detected: ${step.stepType}.${step.method} took ${step.durationMs}ms",
              )
            }
          }

          // Calculate throughput
          if (metrics.durationMs > 0) {
            val recordsPerSecond = (metrics.totalRecordsRead * 1000.0) / metrics.durationMs
            logger.info(f"Throughput: $recordsPerSecond%.2f records/second")
          }

          // Export metrics
          LogExporter.exportAsJson(metrics)
        }

      case Left(_) =>
        logger.error("Pipeline failed")
    }
  }
}
