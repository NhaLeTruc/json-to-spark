package com.pipeline.cli

import com.pipeline.config.PipelineConfigParser
import com.pipeline.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * Main entry point for pipeline execution.
 *
 * Supports both local CLI and cluster execution modes.
 * Implements FR-001: JSON configuration-based execution.
 * Validates Constitution Section V: Library-First Architecture.
 */
object PipelineRunner {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Main method for pipeline execution.
   *
   * Usage:
   *   Local: java -jar pipeline-app.jar config.json
   *   Cluster: spark-submit --class com.pipeline.cli.PipelineRunner pipeline-app-all.jar config.json
   *
   * @param args Command line arguments: [config-file-path]
   */
  def main(args: Array[String]): Unit = {
    logger.info("=== Pipeline Orchestration Application Starting ===")
    logger.info(s"Arguments: ${args.mkString(", ")}")

    // Validate arguments
    if (args.isEmpty) {
      printUsage()
      System.exit(1)
    }

    val configPath = args(0)
    logger.info(s"Loading pipeline configuration from: $configPath")

    try {
      // Parse configuration
      val pipelineConfig = PipelineConfigParser.parseFile(configPath)
      logger.info(s"Loaded pipeline: ${pipelineConfig.name} (mode: ${pipelineConfig.mode})")

      // Create SparkSession
      val spark = createSparkSession(pipelineConfig.mode)

      try {
        // Create pipeline
        val pipeline = Pipeline.fromConfig(pipelineConfig)

        // Register shutdown hook for graceful cancellation
        val shutdownHook = new Thread("pipeline-shutdown-hook") {
          override def run(): Unit = {
            logger.warn("Shutdown signal received, cancelling pipeline...")
            pipeline.cancel()
          }
        }
        Runtime.getRuntime.addShutdownHook(shutdownHook)

        // Execute pipeline
        logger.info(s"Executing pipeline: ${pipelineConfig.name}")
        val startTime = System.currentTimeMillis()

        val result = pipeline.execute(spark)

        // Remove shutdown hook if completed normally
        try Runtime.getRuntime.removeShutdownHook(shutdownHook)
        catch {
          case _: IllegalStateException => // Already shutting down
        }

        result match {
          case Right(context) =>
            val duration = System.currentTimeMillis() - startTime
            logger.info(s"✅ Pipeline completed successfully in ${duration}ms")
            logger.info(s"Final context: ${context.registeredNames.size} DataFrames registered")
            System.exit(0)

          case Left(error) =>
            val duration = System.currentTimeMillis() - startTime
            logger.error(s"❌ Pipeline failed after ${duration}ms", error)
            System.exit(1)
        }

      } finally {
        // Stop SparkSession
        if (!isClusterMode) {
          logger.info("Stopping SparkSession")
          spark.stop()
        }
      }

    } catch {
      case ex: Exception =>
        logger.error("Fatal error during pipeline execution", ex)
        System.exit(1)
    }
  }

  /**
   * Creates SparkSession based on execution mode.
   *
   * @param mode Pipeline mode (batch or streaming)
   * @return Configured SparkSession
   */
  private def createSparkSession(mode: String): SparkSession = {
    logger.info(s"Creating SparkSession for mode: $mode")

    val builder = SparkSession
      .builder()
      .appName("Pipeline Orchestration Application")

    val spark = if (isClusterMode) {
      logger.info("Running in CLUSTER mode - using existing SparkSession")
      builder.getOrCreate()
    } else {
      logger.info("Running in LOCAL mode - creating new SparkSession")
      builder
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    }

    spark.sparkContext.setLogLevel("WARN")

    logger.info(s"SparkSession created: ${spark.version}")
    logger.info(s"Spark master: ${spark.sparkContext.master}")
    logger.info(s"App name: ${spark.sparkContext.appName}")

    spark
  }

  /**
   * Detects if running in cluster mode.
   *
   * @return True if running in cluster, false if local
   */
  private def isClusterMode: Boolean =
    sys.env.contains("SPARK_MASTER") ||
      (sys.props.contains("spark.master") &&
        !sys.props("spark.master").startsWith("local"))

  /**
   * Prints usage information.
   */
  private def printUsage(): Unit =
    println("""
              |Pipeline Orchestration Application
              |
              |Usage:
              |  java -jar pipeline-app.jar <config-file>
              |  spark-submit --class com.pipeline.cli.PipelineRunner pipeline-app-all.jar <config-file>
              |
              |Arguments:
              |  config-file    Path to pipeline JSON configuration file
              |
              |Environment Variables:
              |  VAULT_ADDR     HashiCorp Vault address (e.g., http://localhost:8200)
              |  VAULT_TOKEN    Vault authentication token
              |  VAULT_NAMESPACE (Optional) Vault namespace
              |
              |Examples:
              |  # Local execution
              |  java -jar build/libs/pipeline-app-1.0-SNAPSHOT.jar config/examples/simple-etl.json
              |
              |  # Spark cluster execution
              |  spark-submit --class com.pipeline.cli.PipelineRunner \
              |    --master spark://master:7077 \
              |    build/libs/pipeline-app-1.0-SNAPSHOT-all.jar \
              |    config/examples/simple-etl.json
              |
              |For more information, see README.md
              |""".stripMargin)
}
