package com.pipeline.core

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
 * Pipeline execution context that tracks the primary data flow and registered DataFrames.
 *
 * Supports multi-DataFrame operations for complex transformations (FR-007, FR-023).
 * Supports streaming query management for continuous processing (FR-009).
 * Supports DataFrame caching for performance optimization (FR-020).
 * Implements Constitution Section V: Library-First Architecture.
 *
 * @param primary          Either Avro GenericRecord or Spark DataFrame representing the primary data flow
 * @param dataFrames       Named registry of DataFrames for multi-DataFrame operations
 * @param streamingQueries Named registry of StreamingQuery objects for lifecycle management
 * @param cachedDataFrames Set of names of DataFrames that have been cached
 * @param isStreamingMode  Flag indicating if pipeline is executing in streaming mode
 */
case class PipelineContext(
    primary: Either[GenericRecord, DataFrame],
    dataFrames: Map[String, DataFrame] = Map.empty,
    streamingQueries: Map[String, StreamingQuery] = Map.empty,
    cachedDataFrames: Set[String] = Set.empty,
    isStreamingMode: Boolean = false,
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Registers a DataFrame with a name for later retrieval.
   *
   * Used by ExtractStep with "registerAs" configuration.
   * Enables multi-DataFrame transformations like joins.
   *
   * @param name Name to register the DataFrame under
   * @param df   DataFrame to register
   * @return Updated context with registered DataFrame
   */
  def register(name: String, df: DataFrame): PipelineContext =
    this.copy(dataFrames = dataFrames + (name -> df))

  /**
   * Retrieves a registered DataFrame by name.
   *
   * Used by TransformStep with "inputDataFrames" configuration.
   *
   * @param name Name of the DataFrame to retrieve
   * @return Option containing the DataFrame if found
   */
  def get(name: String): Option[DataFrame] =
    dataFrames.get(name)

  /**
   * Updates the primary data flow.
   *
   * Preserves registered DataFrames in the registry.
   *
   * @param data New primary data (Avro or DataFrame)
   * @return Updated context with new primary data
   */
  def updatePrimary(data: Either[GenericRecord, DataFrame]): PipelineContext =
    this.copy(primary = data)

  /**
   * Gets the primary DataFrame.
   *
   * Throws exception if primary is not a DataFrame (Avro GenericRecord).
   *
   * @return Primary DataFrame
   */
  def getPrimaryDataFrame: DataFrame =
    primary match {
      case Right(df) => df
      case Left(_)   => throw new IllegalStateException("Primary data is Avro GenericRecord, not DataFrame")
    }

  /**
   * Checks if primary data is a DataFrame.
   *
   * @return True if primary is DataFrame, false if Avro
   */
  def isPrimaryDataFrame: Boolean = primary.isRight

  /**
   * Checks if primary data is Avro GenericRecord.
   *
   * @return True if primary is Avro, false if DataFrame
   */
  def isPrimaryAvro: Boolean = primary.isLeft

  /**
   * Gets all registered DataFrame names.
   *
   * @return Set of registered DataFrame names
   */
  def registeredNames: Set[String] = dataFrames.keySet

  /**
   * Clears all registered DataFrames.
   *
   * Primary data is preserved.
   *
   * @return Updated context with empty DataFrame registry
   */
  def clearRegistry(): PipelineContext =
    this.copy(dataFrames = Map.empty)

  /**
   * Registers a streaming query for lifecycle management.
   *
   * Used by LoadStep when writing streams to sinks.
   *
   * @param name  Name to register the query under
   * @param query StreamingQuery instance
   * @return Updated context
   */
  def registerStreamingQuery(name: String, query: StreamingQuery): PipelineContext = {
    logger.info(s"Registered streaming query: $name (id=${query.id})")
    this.copy(streamingQueries = streamingQueries + (name -> query))
  }

  /**
   * Retrieves a registered streaming query by name.
   *
   * @param name Name of the query to retrieve
   * @return Option containing the StreamingQuery if found
   */
  def getStreamingQuery(name: String): Option[StreamingQuery] =
    streamingQueries.get(name)

  /**
   * Gets all registered streaming query names.
   *
   * @return Set of registered query names
   */
  def streamingQueryNames: Set[String] = streamingQueries.keySet

  /**
   * Stops all running streaming queries.
   *
   * Called during pipeline shutdown or cancellation.
   */
  def stopAllStreams(): Unit =
    if (streamingQueries.nonEmpty) {
      logger.info(s"Stopping ${streamingQueries.size} streaming queries")
      streamingQueries.values.foreach { query =>
        if (query.isActive) {
          logger.info(s"Stopping query: ${query.name} (id=${query.id})")
          query.stop()
        }
      }
    }

  /**
   * Awaits termination of all streaming queries.
   *
   * @param timeout Optional timeout in milliseconds
   */
  def awaitTermination(timeout: Option[Long] = None): Unit = {
    if (streamingQueries.isEmpty) {
      logger.warn("No streaming queries to await")
      return
    }

    timeout match {
      case Some(ms) =>
        logger.info(s"Awaiting termination for ${ms}ms")
        streamingQueries.values.foreach(_.awaitTermination(ms))
      case None     =>
        logger.info("Awaiting termination (indefinite)")
        streamingQueries.values.foreach(_.awaitTermination())
    }
  }

  /**
   * Checks if any streaming queries are currently active.
   *
   * @return True if at least one query is active
   */
  def hasActiveStreams: Boolean =
    streamingQueries.values.exists(_.isActive)

  /**
   * Caches a DataFrame with the specified storage level.
   *
   * @param name         Name of the DataFrame to cache
   * @param storageLevel Storage level (default: MEMORY_AND_DISK)
   * @return Updated context
   */
  def cache(name: String, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): PipelineContext =
    dataFrames.get(name) match {
      case Some(df) =>
        logger.info(s"Caching DataFrame '$name' with storage level: $storageLevel")
        df.persist(storageLevel)
        this.copy(cachedDataFrames = cachedDataFrames + name)
      case None     =>
        logger.warn(s"Cannot cache DataFrame '$name' - not found in registry")
        this
    }

  /**
   * Caches the primary DataFrame with the specified storage level.
   *
   * @param storageLevel Storage level (default: MEMORY_AND_DISK)
   * @return Updated context
   */
  def cachePrimary(storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): PipelineContext =
    primary match {
      case Right(df) =>
        logger.info(s"Caching primary DataFrame with storage level: $storageLevel")
        df.persist(storageLevel)
        this.copy(cachedDataFrames = cachedDataFrames + "__primary__")
      case Left(_)   =>
        logger.warn("Cannot cache primary - it's an Avro GenericRecord, not a DataFrame")
        this
    }

  /**
   * Uncaches a DataFrame and removes it from memory.
   *
   * @param name Name of the DataFrame to uncache
   * @return Updated context
   */
  def uncache(name: String): PipelineContext =
    dataFrames.get(name) match {
      case Some(df) =>
        logger.info(s"Uncaching DataFrame '$name'")
        df.unpersist(blocking = false)
        this.copy(cachedDataFrames = cachedDataFrames - name)
      case None     =>
        logger.warn(s"Cannot uncache DataFrame '$name' - not found in registry")
        this
    }

  /**
   * Uncaches all cached DataFrames.
   *
   * @param blocking Whether to block until unpersist completes (default: false)
   * @return Updated context
   */
  def uncacheAll(blocking: Boolean = false): PipelineContext = {
    if (cachedDataFrames.nonEmpty) {
      logger.info(s"Uncaching ${cachedDataFrames.size} DataFrames")

      cachedDataFrames.foreach { name =>
        if (name == "__primary__") {
          primary match {
            case Right(df) => df.unpersist(blocking)
            case Left(_)   => // Skip Avro
          }
        } else {
          dataFrames.get(name).foreach(_.unpersist(blocking))
        }
      }
    }
    this.copy(cachedDataFrames = Set.empty)
  }

  /**
   * Checks if a DataFrame is cached.
   *
   * @param name Name of the DataFrame
   * @return True if cached
   */
  def isCached(name: String): Boolean =
    cachedDataFrames.contains(name)

  /**
   * Gets all cached DataFrame names.
   *
   * @return Set of cached DataFrame names
   */
  def cachedNames: Set[String] =
    cachedDataFrames
}

/**
 * Factory methods for PipelineContext creation.
 */
object PipelineContext {

  /**
   * Creates a context from a DataFrame.
   *
   * @param df Primary DataFrame
   * @return New PipelineContext
   */
  def fromDataFrame(df: DataFrame): PipelineContext =
    PipelineContext(Right(df))

  /**
   * Creates a context from an Avro GenericRecord.
   *
   * @param record Primary Avro record
   * @return New PipelineContext
   */
  def fromAvro(record: GenericRecord): PipelineContext =
    PipelineContext(Left(record))
}
