package com.pipeline.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import com.pipeline.operations.{ExtractMethods, LoadMethods, UserMethods}
import com.pipeline.exceptions._

/**
 * Sealed trait for pipeline steps implementing Chain of Responsibility pattern.
 *
 * Implements FR-006: Chain of Responsibility for pipeline steps.
 * Validates Constitution Section I: SOLID principles (Open/Closed Principle).
 *
 * Each step can execute its logic and optionally chain to the next step.
 */
sealed trait PipelineStep {

  /** Method name to execute (e.g., "fromPostgres", "filterRows", "toS3") */
  def method: String

  /** Configuration for this step */
  def config: Map[String, Any]

  /** Next step in the chain (None if this is the last step) */
  def nextStep: Option[PipelineStep]

  /**
   * Executes this step's logic.
   *
   * @param context Current pipeline context
   * @param spark   SparkSession
   * @return Updated pipeline context
   */
  def execute(context: PipelineContext, spark: SparkSession): PipelineContext

  /**
   * Executes this step and chains to next step if present.
   *
   * Implements Chain of Responsibility pattern with exception handling.
   *
   * @param context Current pipeline context
   * @param spark   SparkSession
   * @return Final pipeline context after chain execution
   * @throws PipelineException if execution fails
   */
  def executeChain(context: PipelineContext, spark: SparkSession): PipelineContext =
    executeChainWithContext(context, spark, pipelineName = None, stepIndex = None)

  /**
   * Internal method for executing chain with context information.
   *
   * @param context       Current pipeline context
   * @param spark         SparkSession
   * @param pipelineName  Name of the pipeline (for error context)
   * @param stepIndex     Index of this step (for error context)
   * @param metricsCollector Optional metrics collector
   * @return Final pipeline context after chain execution
   * @throws PipelineException if execution fails
   */
  private[core] def executeChainWithContext(
      context: PipelineContext,
      spark: SparkSession,
      pipelineName: Option[String],
      stepIndex: Option[Int],
      metricsCollector: Option[com.pipeline.metrics.PipelineMetrics] = None,
  ): PipelineContext = {
    val logger           = LoggerFactory.getLogger(getClass)
    val stepType         = getStepType
    val currentStepIndex = stepIndex.getOrElse(0)

    logger.info(s"Executing step: ${this.getClass.getSimpleName}, method: $method")

    // Record step start
    metricsCollector.foreach(_.startStep(currentStepIndex, stepType, method))

    try {
      val updatedContext = execute(context, spark)

      // Record step completion (basic metrics - can be enhanced with actual counts)
      metricsCollector.foreach(_.endStep(currentStepIndex))

      nextStep match {
        case Some(next) =>
          logger.info(s"Chaining to next step: ${next.getClass.getSimpleName}")
          val nextIndex = stepIndex.map(_ + 1).orElse(Some(1))
          next.executeChainWithContext(updatedContext, spark, pipelineName, nextIndex, metricsCollector)

        case None =>
          logger.info("End of chain reached")
          updatedContext
      }
    } catch {
      case pe: PipelineException =>
        // Record step failure
        metricsCollector.foreach(_.failStep(currentStepIndex, pe.getMessage))
        // Already a pipeline exception, just re-throw
        throw pe

      case ex: Throwable =>
        // Record step failure
        metricsCollector.foreach(_.failStep(currentStepIndex, ex.getMessage))
        // Wrap with pipeline context
        logger.error(s"Error executing step $stepType.$method", ex)
        throw PipelineException.wrapException(
          ex,
          pipelineName = pipelineName,
          stepIndex = stepIndex,
          stepType = Some(stepType),
          method = Some(method),
          config = Some(config),
        )
    }
  }

  /**
   * Get the step type name (extract, transform, validate, load).
   */
  private def getStepType: String = this match {
    case _: ExtractStep   => "extract"
    case _: TransformStep => "transform"
    case _: ValidateStep  => "validate"
    case _: LoadStep      => "load"
  }
}

/**
 * Extract step for reading data from sources.
 *
 * Implements FR-003: Extract from data sources.
 * Supports PostgreSQL, MySQL, Kafka, S3, DeltaLake (FR-023).
 *
 * @param method   Extract method name (e.g., "fromPostgres")
 * @param config   Configuration including table/query, credentials
 * @param nextStep Optional next step in chain
 */
case class ExtractStep(
    method: String,
    config: Map[String, Any],
    nextStep: Option[PipelineStep],
) extends PipelineStep {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def execute(context: PipelineContext, spark: SparkSession): PipelineContext = {
    logger.info(s"Extract step executing: method=$method")

    // Check if we're in streaming mode from context metadata
    val isStreaming = context.isStreamingMode

    // Extract logic delegated to ExtractMethods
    val df = extractData(spark, isStreaming)

    // Validate extraction if configured
    val failOnEmpty = config.getOrElse("failOnEmpty", false).toString.toBoolean
    if (failOnEmpty && !df.isStreaming) {
      // Check if DataFrame is empty (only for batch mode)
      val isEmpty = df.head(1).isEmpty
      if (isEmpty) {
        throw new PipelineExecutionException(
          s"Extraction produced no data and failOnEmpty=true for method: $method",
          stepType = Some("extract"),
          method = Some(method),
          config = Some(config),
        )
      }
      logger.info("DataFrame validation passed: data exists")
    }

    // Register DataFrame if registerAs is specified
    val contextWithDF = config.get("registerAs") match {
      case Some(name) =>
        logger.info(s"Registering DataFrame as: $name")
        context.register(name.toString, df).updatePrimary(Right(df))
      case None       =>
        context.updatePrimary(Right(df))
    }

    // Cache DataFrame if cache is enabled
    val updatedContext = config.get("cache") match {
      case Some(true) | Some("true") =>
        val registerName = config.get("registerAs").map(_.toString).getOrElse("__primary__")
        val storageLevel =
          PipelineStepUtils.parseStorageLevel(config.getOrElse("cacheStorageLevel", "MEMORY_AND_DISK").toString)

        logger.info(s"Caching DataFrame with storage level: $storageLevel")
        if (registerName == "__primary__") {
          contextWithDF.cachePrimary(storageLevel)
        } else {
          contextWithDF.cache(registerName, storageLevel)
        }

      case _ =>
        contextWithDF
    }

    updatedContext
  }

  /**
   * Delegates to ExtractMethods based on method name.
   */
  private def extractData(spark: SparkSession, isStreaming: Boolean): DataFrame =
    method match {
      case "fromPostgres"  => ExtractMethods.fromPostgres(config, spark)
      case "fromMySQL"     => ExtractMethods.fromMySQL(config, spark)
      case "fromKafka"     => ExtractMethods.fromKafka(config, spark, isStreaming)
      case "fromS3"        => ExtractMethods.fromS3(config, spark)
      case "fromDeltaLake" => ExtractMethods.fromDeltaLake(config, spark)
      case "fromAvro"      => ExtractMethods.fromAvro(config, spark)
      case _               => throw new IllegalArgumentException(s"Unknown extract method: $method")
    }
}

/**
 * Transform step for data transformation operations.
 *
 * Implements FR-004: Transform operations.
 * Supports filterRows, joinDataFrames, aggregateData, etc. (FR-024).
 *
 * @param method   Transform method name (e.g., "filterRows")
 * @param config   Configuration including transformation logic
 * @param nextStep Optional next step in chain
 */
case class TransformStep(
    method: String,
    config: Map[String, Any],
    nextStep: Option[PipelineStep],
) extends PipelineStep {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def execute(context: PipelineContext, spark: SparkSession): PipelineContext = {
    logger.info(s"Transform step executing: method=$method")

    // Resolve input DataFrames if specified (multi-DataFrame support)
    val enrichedConfig = config.get("inputDataFrames") match {
      case Some(names: List[_]) =>
        val dfNames = names.map(_.toString)
        logger.info(s"Resolving input DataFrames: ${dfNames.mkString(", ")}")

        val resolvedDFs = dfNames.flatMap { name =>
          context.get(name).map(df => name -> df)
        }.toMap

        if (resolvedDFs.size != dfNames.size) {
          val missing   = dfNames.filterNot(resolvedDFs.contains)
          val available = context.dataFrames.keySet.toSet
          // Throw first missing DataFrame as exception
          throw new DataFrameResolutionException(
            referenceName = missing.head,
            availableNames = available,
            stepType = Some("transform"),
            method = Some(method),
          )
        }

        config ++ Map("resolvedDataFrames" -> resolvedDFs)

      case _                    =>
        config
    }

    // Transform logic will be implemented by UserMethods
    val transformedDf = transformData(context.getPrimaryDataFrame, enrichedConfig, spark)

    // Register result if registerAs is specified
    val updatedContext = config.get("registerAs") match {
      case Some(name) =>
        logger.info(s"Registering transformed DataFrame as: $name")
        context.register(name.toString, transformedDf).updatePrimary(Right(transformedDf))
      case None       =>
        context.updatePrimary(Right(transformedDf))
    }

    updatedContext
  }

  /**
   * Delegates to UserMethods based on method name.
   */
  private def transformData(df: DataFrame, cfg: Map[String, Any], spark: SparkSession): DataFrame =
    method match {
      case "filterRows"           => UserMethods.filterRows(df, cfg, spark)
      case "enrichData"           => UserMethods.enrichData(df, cfg, spark)
      case "joinDataFrames"       => UserMethods.joinDataFrames(df, cfg, spark)
      case "aggregateData"        => UserMethods.aggregateData(df, cfg, spark)
      case "reshapeData"          => UserMethods.reshapeData(df, cfg, spark)
      case "unionDataFrames"      => UserMethods.unionDataFrames(df, cfg, spark)
      case "toAvroSchema"         => UserMethods.toAvroSchema(df, cfg, spark)
      case "evolveAvroSchema"     => UserMethods.evolveAvroSchema(df, cfg, spark)
      case "repartition"          => UserMethods.repartition(df, cfg, spark)
      case "repartitionByColumns" => UserMethods.repartitionByColumns(df, cfg, spark)
      case "coalesce"             => UserMethods.coalesce(df, cfg, spark)
      case _                      => throw new IllegalArgumentException(s"Unknown transform method: $method")
    }
}

/**
 * Validate step for data quality validation.
 *
 * Implements FR-010: Data validation.
 * Supports schema, nulls, ranges, referential integrity, business rules (FR-025).
 *
 * @param method   Validation method name (e.g., "validateSchema")
 * @param config   Configuration including validation rules
 * @param nextStep Optional next step in chain
 */
case class ValidateStep(
    method: String,
    config: Map[String, Any],
    nextStep: Option[PipelineStep],
) extends PipelineStep {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def execute(context: PipelineContext, spark: SparkSession): PipelineContext = {
    logger.info(s"Validate step executing: method=$method")

    val df = context.getPrimaryDataFrame

    // Resolve referenced DataFrames if needed (for referential integrity)
    val enrichedConfig = config.get("referencedDataFrame") match {
      case Some(refName: String) =>
        logger.info(s"Resolving referenced DataFrame: $refName")
        val refDf = context.get(refName).getOrElse {
          val available = context.dataFrames.keySet.toSet
          throw new DataFrameResolutionException(
            referenceName = refName,
            availableNames = available,
            stepType = Some("validate"),
            method = Some(method),
          )
        }
        config ++ Map("resolvedReferencedDataFrame" -> refDf)
      case _                     =>
        config
    }

    // Validation logic will be implemented by UserMethods
    validateData(df, enrichedConfig, spark)

    // Validation doesn't modify data, just checks it
    context
  }

  /**
   * Delegates to UserMethods validation methods based on method name.
   * Throws exception if validation fails.
   */
  private def validateData(df: DataFrame, cfg: Map[String, Any], spark: SparkSession): Unit =
    method match {
      case "validateSchema"               => UserMethods.validateSchema(df, cfg, spark)
      case "validateNulls"                => UserMethods.validateNulls(df, cfg, spark)
      case "validateRanges"               => UserMethods.validateRanges(df, cfg, spark)
      case "validateReferentialIntegrity" => UserMethods.validateReferentialIntegrity(df, cfg, spark)
      case "validateBusinessRules"        => UserMethods.validateBusinessRules(df, cfg, spark)
      case _                              => throw new IllegalArgumentException(s"Unknown validation method: $method")
    }
}

/**
 * Load step for writing data to sinks.
 *
 * Implements FR-005: Load to data sinks.
 * Supports PostgreSQL, MySQL, Kafka, S3, DeltaLake (FR-023).
 *
 * @param method   Load method name (e.g., "toS3")
 * @param config   Configuration including destination, credentials
 * @param nextStep Optional next step in chain (rarely used for Load)
 */
case class LoadStep(
    method: String,
    config: Map[String, Any],
    nextStep: Option[PipelineStep],
) extends PipelineStep {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def execute(context: PipelineContext, spark: SparkSession): PipelineContext = {
    logger.info(s"Load step executing: method=$method")

    val df          = context.getPrimaryDataFrame
    val isStreaming = context.isStreamingMode

    // Load logic delegated to LoadMethods
    val maybeQuery = loadData(df, spark, isStreaming)

    // Register streaming query if present
    val updatedContext = maybeQuery match {
      case Some(query) =>
        // Generate query name from config or use method-based default
        val queryName = config.get("queryName") match {
          case Some(name) => name.toString
          case None       => s"${method}_${java.util.UUID.randomUUID().toString.take(8)}"
        }
        logger.info(s"Registering streaming query: $queryName")
        context.registerStreamingQuery(queryName, query)
      case None        =>
        logger.info(s"Batch load completed: $method")
        context
    }

    updatedContext
  }

  /**
   * Delegates to LoadMethods based on method name.
   * Returns Option[StreamingQuery] for streaming-capable methods.
   */
  private def loadData(
      df: DataFrame,
      spark: SparkSession,
      isStreaming: Boolean,
  ): Option[org.apache.spark.sql.streaming.StreamingQuery] =
    method match {
      case "toPostgres"  => LoadMethods.toPostgres(df, config, spark); None
      case "toMySQL"     => LoadMethods.toMySQL(df, config, spark); None
      case "toKafka"     => LoadMethods.toKafka(df, config, spark, isStreaming)
      case "toS3"        => LoadMethods.toS3(df, config, spark); None
      case "toDeltaLake" => LoadMethods.toDeltaLake(df, config, spark, isStreaming)
      case "toAvro"      => LoadMethods.toAvro(df, config, spark); None
      case _             => throw new IllegalArgumentException(s"Unknown load method: $method")
    }

}

/**
 * Helper object for pipeline step utilities.
 */
object PipelineStepUtils {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Parses storage level string to StorageLevel object.
   *
   * @param level Storage level string (e.g., "MEMORY_AND_DISK")
   * @return StorageLevel object
   */
  def parseStorageLevel(level: String): StorageLevel =
    level.toUpperCase match {
      case "NONE"                  => StorageLevel.NONE
      case "DISK_ONLY"             => StorageLevel.DISK_ONLY
      case "DISK_ONLY_2"           => StorageLevel.DISK_ONLY_2
      case "MEMORY_ONLY"           => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_2"         => StorageLevel.MEMORY_ONLY_2
      case "MEMORY_ONLY_SER"       => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_ONLY_SER_2"     => StorageLevel.MEMORY_ONLY_SER_2
      case "MEMORY_AND_DISK"       => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_2"     => StorageLevel.MEMORY_AND_DISK_2
      case "MEMORY_AND_DISK_SER"   => StorageLevel.MEMORY_AND_DISK_SER
      case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
      case "OFF_HEAP"              => StorageLevel.OFF_HEAP
      case invalid                 =>
        val validLevels = List(
          "NONE",
          "DISK_ONLY",
          "DISK_ONLY_2",
          "MEMORY_ONLY",
          "MEMORY_ONLY_2",
          "MEMORY_ONLY_SER",
          "MEMORY_ONLY_SER_2",
          "MEMORY_AND_DISK",
          "MEMORY_AND_DISK_2",
          "MEMORY_AND_DISK_SER",
          "MEMORY_AND_DISK_SER_2",
          "OFF_HEAP",
        )
        throw new IllegalArgumentException(
          s"Invalid storage level: '$invalid'. Valid values: ${validLevels.mkString(", ")}",
        )
    }
}
