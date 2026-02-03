package com.pipeline.avro

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataType
import org.apache.avro.Schema
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

/**
 * Utilities for converting between Spark DataFrames and Avro format.
 *
 * Supports:
 * - DataFrame to Avro serialization
 * - Avro to DataFrame deserialization
 * - Schema evolution and compatibility checking
 * - Multiple Avro file formats (avro, parquet with Avro schema)
 *
 * Implements Phase 9: Avro Conversion (T114-T121)
 */
object AvroConverter {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Converts a DataFrame to Avro format and writes to path.
   *
   * @param df     DataFrame to convert
   * @param path   Output path (local, S3, HDFS)
   * @param config Configuration options
   * @param spark  SparkSession
   */
  def writeAvro(df: DataFrame, path: String, config: Map[String, Any] = Map.empty)(implicit
      spark: SparkSession,
  ): Unit = {
    logger.info(s"Writing DataFrame to Avro format at: $path")

    val writer = df.write
      .format("avro")
      .mode(config.getOrElse("mode", "append").toString)

    // Add partitioning if specified
    val partitionedWriter = config.get("partitionBy") match {
      case Some(columns: List[_]) => writer.partitionBy(columns.map(_.toString): _*)
      case Some(column: String)   => writer.partitionBy(column)
      case _                      => writer
    }

    // Add compression if specified
    val compressedWriter = config.get("compression") match {
      case Some(codec) => partitionedWriter.option("compression", codec.toString)
      case None        => partitionedWriter
    }

    // Add custom Avro schema if specified
    val finalWriter = config.get("avroSchema") match {
      case Some(schema: String) => compressedWriter.option("avroSchema", schema)
      case _                    => compressedWriter
    }

    finalWriter.save(path)
    logger.info(s"Successfully wrote DataFrame to Avro at: $path")
  }

  /**
   * Reads Avro format from path and converts to DataFrame.
   *
   * @param path   Input path (local, S3, HDFS)
   * @param config Configuration options
   * @param spark  SparkSession
   * @return DataFrame containing Avro data
   */
  def readAvro(path: String, config: Map[String, Any] = Map.empty)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading Avro format from: $path")

    val reader = spark.read.format("avro")

    // Add schema inference mode if specified
    val schemaReader = config.get("inferSchema") match {
      case Some(infer: Boolean) => reader.option("inferSchema", infer.toString)
      case Some(infer: String)  => reader.option("inferSchema", infer)
      case _                    => reader
    }

    // Add custom Avro schema if specified
    val finalReader = config.get("avroSchema") match {
      case Some(schema: String) => schemaReader.option("avroSchema", schema)
      case _                    => schemaReader
    }

    val df = finalReader.load(path)
    logger.info(s"Successfully read Avro data from: $path")
    df
  }

  /**
   * Converts DataFrame schema to Avro schema JSON string.
   *
   * @param df        DataFrame to extract schema from
   * @param namespace Optional namespace for Avro schema
   * @param name      Schema name (defaults to "Record")
   * @return Avro schema as JSON string
   */
  def dataFrameToAvroSchema(df: DataFrame, namespace: String = "com.pipeline", name: String = "Record"): String = {
    logger.info(s"Converting DataFrame schema to Avro schema: $namespace.$name")

    // Use Spark's built-in Avro conversion
    import org.apache.spark.sql.avro.SchemaConverters

    val avroType   = SchemaConverters.toAvroType(df.schema, nullable = false, name, namespace)
    val avroSchema = avroType.toString

    logger.debug(s"Generated Avro schema: $avroSchema")
    avroSchema
  }

  /**
   * Validates schema compatibility between two Avro schemas.
   *
   * Checks if readerSchema can read data written with writerSchema.
   *
   * @param writerSchema Schema used to write data
   * @param readerSchema Schema to use for reading data
   * @return Success(true) if compatible, Failure with error if incompatible
   */
  def validateSchemaCompatibility(writerSchema: String, readerSchema: String): Try[Boolean] = Try {
    logger.info("Validating Avro schema compatibility")

    val writerAvroSchema = new Schema.Parser().parse(writerSchema)
    val readerAvroSchema = new Schema.Parser().parse(readerSchema)

    // Basic compatibility check
    // In production, use org.apache.avro.SchemaCompatibility for comprehensive checks
    val compatible = checkBasicCompatibility(writerAvroSchema, readerAvroSchema)

    if (compatible) {
      logger.info("Schemas are compatible")
      true
    } else {
      logger.error("Schemas are NOT compatible")
      throw new IllegalArgumentException("Schema compatibility validation failed")
    }
  }

  /**
   * Performs basic schema compatibility check.
   *
   * @param writerSchema Writer's Avro schema
   * @param readerSchema Reader's Avro schema
   * @return true if compatible, false otherwise
   */
  private def checkBasicCompatibility(writerSchema: Schema, readerSchema: Schema): Boolean = {
    // Check type compatibility
    if (writerSchema.getType != readerSchema.getType) {
      logger.error(s"Type mismatch: writer=${writerSchema.getType}, reader=${readerSchema.getType}")
      return false
    }

    // For records, check field compatibility
    if (writerSchema.getType == Schema.Type.RECORD) {
      val writerFields = writerSchema.getFields
      val readerFields = readerSchema.getFields

      // Reader can have fewer fields (forward compatibility)
      // Reader can have additional fields with defaults (backward compatibility)

      import scala.jdk.CollectionConverters._

      val writerFieldNames = writerFields.asScala.map(_.name()).toSet
      val readerFieldNames = readerFields.asScala.map(_.name()).toSet

      // Check that all reader fields exist in writer or have defaults
      readerFields.asScala.foreach { readerField =>
        if (!writerFieldNames.contains(readerField.name())) {
          if (readerField.defaultVal() == null) {
            logger.error(s"Reader field '${readerField.name()}' not in writer schema and has no default")
            return false
          } else {
            logger.debug(s"Reader field '${readerField.name()}' has default value, compatible")
          }
        }
      }
    }

    true
  }

  /**
   * Evolves DataFrame schema to match target Avro schema.
   *
   * Adds missing columns with null values, removes extra columns.
   *
   * @param df           DataFrame to evolve
   * @param targetSchema Target Avro schema as JSON string
   * @param spark        SparkSession
   * @return DataFrame with evolved schema
   */
  def evolveSchema(df: DataFrame, targetSchema: String)(implicit spark: SparkSession): DataFrame = {
    logger.info("Evolving DataFrame schema to match target Avro schema")

    val targetAvroSchema = new Schema.Parser().parse(targetSchema)

    import scala.jdk.CollectionConverters._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val currentColumns = df.columns.toSet
    val targetFields   = targetAvroSchema.getFields.asScala

    var evolvedDf = df

    // Add missing columns with null values
    targetFields.foreach { field =>
      if (!currentColumns.contains(field.name())) {
        logger.info(s"Adding missing column: ${field.name()}")

        // Determine Spark type from Avro type
        val sparkType = avroTypeToSparkType(field.schema())
        evolvedDf = evolvedDf.withColumn(field.name(), lit(null).cast(sparkType))
      }
    }

    // Remove extra columns not in target schema
    val targetColumnNames = targetFields.map(_.name()).toSet
    val columnsToKeep     = evolvedDf.columns.filter(targetColumnNames.contains)

    if (columnsToKeep.length < evolvedDf.columns.length) {
      val removed = evolvedDf.columns.filterNot(targetColumnNames.contains)
      logger.info(s"Removing extra columns: ${removed.mkString(", ")}")
    }

    if (columnsToKeep.isEmpty) {
      throw new IllegalArgumentException("No columns match target schema")
    }
    evolvedDf.select(columnsToKeep.head, columnsToKeep.tail: _*)
  }

  /**
   * Converts Avro schema type to Spark DataType.
   *
   * @param avroSchema Avro schema
   * @return Corresponding Spark DataType
   */
  private def avroTypeToSparkType(avroSchema: Schema): DataType = {
    import org.apache.spark.sql.types._

    avroSchema.getType match {
      case Schema.Type.STRING  => StringType
      case Schema.Type.INT     => IntegerType
      case Schema.Type.LONG    => LongType
      case Schema.Type.FLOAT   => FloatType
      case Schema.Type.DOUBLE  => DoubleType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.BYTES   => BinaryType
      case Schema.Type.NULL    => NullType
      case Schema.Type.UNION   =>
        // Handle nullable types (union of [null, type])
        import scala.jdk.CollectionConverters._
        val types = avroSchema.getTypes.asScala
        types.find(_.getType != Schema.Type.NULL) match {
          case Some(nonNullType) => avroTypeToSparkType(nonNullType)
          case None              => NullType
        }
      case _                   => StringType // Default fallback
    }
  }

  /**
   * Writes DataFrame to Parquet with embedded Avro schema.
   *
   * Useful for efficient storage with Avro metadata.
   *
   * @param df     DataFrame to write
   * @param path   Output path
   * @param config Configuration options
   * @param spark  SparkSession
   */
  def writeParquetWithAvroSchema(df: DataFrame, path: String, config: Map[String, Any] = Map.empty)(implicit
      spark: SparkSession,
  ): Unit = {
    logger.info(s"Writing DataFrame to Parquet with Avro schema at: $path")

    // Generate Avro schema
    val avroSchema = dataFrameToAvroSchema(df)

    val writer = df.write
      .format("parquet")
      .mode(config.getOrElse("mode", "append").toString)
      .option("avroSchema", avroSchema)

    // Add partitioning if specified
    val partitionedWriter = config.get("partitionBy") match {
      case Some(columns: List[_]) => writer.partitionBy(columns.map(_.toString): _*)
      case Some(column: String)   => writer.partitionBy(column)
      case _                      => writer
    }

    // Add compression
    val compression = config.getOrElse("compression", "snappy").toString
    val finalWriter = partitionedWriter.option("compression", compression)

    finalWriter.save(path)
    logger.info(s"Successfully wrote DataFrame to Parquet with Avro schema at: $path")
  }
}
