package com.pipeline.operations

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import com.pipeline.credentials.{CredentialConfigFactory, CredentialResolver, IAMConfig, JdbcConfig, OtherConfig, VaultClient}
import com.pipeline.avro.AvroConverter
import scala.util.{Failure, Success}

/**
 * Static methods for loading data to various sinks.
 *
 * Implements FR-005: Load to data sinks.
 * Implements FR-023: Support at least 5 load methods.
 * Validates Constitution Section V: Library-First Architecture.
 */
object LoadMethods {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Loads data to PostgreSQL.
   *
   * @param df     DataFrame to load
   * @param config Configuration including table, mode, credentials
   * @param spark  SparkSession
   */
  def toPostgres(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Loading to PostgreSQL")

    require(config.contains("table"), "'table' is required")

    val jdbcConfig = resolveJdbcCredentials(config, "postgres")
    val saveMode   = parseSaveMode(config.getOrElse("mode", "append").toString)

    val writer = df.write
      .format("jdbc")
      .option("url", jdbcConfig.jdbcUrl)
      .option("user", jdbcConfig.username)
      .option("password", jdbcConfig.password)
      .option("dbtable", config("table").toString)
      .option("driver", "org.postgresql.Driver")
      .mode(saveMode)

    // Add batch size for performance
    val batchSize = config.getOrElse("batchSize", 1000).toString.toInt
    writer.option("batchsize", batchSize.toString)

    // Add additional JDBC properties if provided
    val finalWriter = jdbcConfig.properties.foldLeft(writer) { case (w, (key, value)) =>
      w.option(key, value)
    }

    finalWriter.save()
    logger.info(s"Loaded data to PostgreSQL table ${config("table")}")
  }

  /**
   * Loads data to MySQL.
   *
   * @param df     DataFrame to load
   * @param config Configuration including table, mode, credentials
   * @param spark  SparkSession
   */
  def toMySQL(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Loading to MySQL")

    require(config.contains("table"), "'table' is required")

    val jdbcConfig = resolveJdbcCredentials(config, "mysql")
    val saveMode   = parseSaveMode(config.getOrElse("mode", "append").toString)

    val writer = df.write
      .format("jdbc")
      .option("url", jdbcConfig.jdbcUrl)
      .option("user", jdbcConfig.username)
      .option("password", jdbcConfig.password)
      .option("dbtable", config("table").toString)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .mode(saveMode)

    // Add batch size for performance
    val batchSize = config.getOrElse("batchSize", 1000).toString.toInt
    writer.option("batchsize", batchSize.toString)

    // Add additional JDBC properties if provided
    val finalWriter = jdbcConfig.properties.foldLeft(writer) { case (w, (key, value)) =>
      w.option(key, value)
    }

    finalWriter.save()
    logger.info(s"Loaded data to MySQL table ${config("table")}")
  }

  /**
   * Loads data to Kafka.
   *
   * @param df          DataFrame to load
   * @param config      Configuration including topic, credentials
   * @param spark       SparkSession
   * @param isStreaming Whether to use streaming write
   * @return Option[StreamingQuery] if streaming, None if batch
   */
  def toKafka(
      df: DataFrame,
      config: Map[String, Any],
      spark: SparkSession,
      isStreaming: Boolean = false,
  ): Option[org.apache.spark.sql.streaming.StreamingQuery] = {
    val mode = if (isStreaming) "STREAMING" else "BATCH"
    logger.info(s"Loading to Kafka in $mode mode")

    require(config.contains("topic"), "'topic' is required")

    val kafkaConfig      = resolveKafkaCredentials(config)
    val topic            = config("topic").toString
    val bootstrapServers = kafkaConfig.getOrElse("bootstrap.servers", "localhost:9092").toString

    // DataFrame must have 'key' and 'value' columns for Kafka
    // If not present, create them
    val kafkaDF = if (df.columns.contains("key") && df.columns.contains("value")) {
      df
    } else {
      import org.apache.spark.sql.functions._
      df.withColumn("key", lit(null).cast("string"))
        .withColumn("value", to_json(struct(df.columns.map(col): _*)))
    }

    val preparedDF = kafkaDF.select("key", "value")

    if (isStreaming) {
      // Streaming write
      logger.info(s"Writing stream to Kafka topic: $topic")

      val checkpointLocation = config
        .getOrElse("checkpointLocation", s"/tmp/checkpoints/kafka_${java.util.UUID.randomUUID().toString}")
        .toString

      val outputMode      = config.getOrElse("outputMode", "append").toString
      val triggerInterval = config.getOrElse("trigger", "5 seconds").toString

      var streamWriter = preparedDF.writeStream
        .format("kafka")
        .outputMode(outputMode)
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", topic)
        .option("checkpointLocation", checkpointLocation)

      // Add additional Kafka properties if provided
      kafkaConfig.foreach { case (key, value) =>
        if (key != "bootstrap.servers") {
          streamWriter = streamWriter.option(s"kafka.$key", value.toString)
        }
      }

      // Parse trigger
      val trigger = parseTrigger(triggerInterval)

      val query = streamWriter.trigger(trigger).start()
      logger.info(s"Started streaming query to Kafka: name=${query.name}, id=${query.id}")
      Some(query)

    } else {
      // Batch write
      var writer = preparedDF.write
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", topic)

      // Add additional Kafka properties if provided
      kafkaConfig.foreach { case (key, value) =>
        if (key != "bootstrap.servers") {
          writer = writer.option(s"kafka.$key", value.toString)
        }
      }

      writer.save()
      logger.info(s"Loaded data to Kafka topic $topic")
      None
    }
  }

  /**
   * Loads data to S3.
   *
   * @param df     DataFrame to load
   * @param config Configuration including bucket, path, format, credentials
   * @param spark  SparkSession
   */
  def toS3(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Loading to S3")

    require(config.contains("bucket"), "'bucket' is required")
    require(config.contains("path"), "'path' is required")

    val iamConfig = resolveS3Credentials(config)

    // Configure S3A credentials
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", iamConfig.accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", iamConfig.secretAccessKey)
    iamConfig.sessionToken.foreach { token =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", token)
    }
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val format   = config.getOrElse("format", "parquet").toString
    val saveMode = parseSaveMode(config.getOrElse("mode", "append").toString)
    val s3Path   = s"s3a://${config("bucket")}${config("path")}"

    val writer = df.write.format(format).mode(saveMode)

    // Add partitioning if specified
    val finalWriter = config.get("partitionBy") match {
      case Some(columns: List[_]) => writer.partitionBy(columns.map(_.toString): _*)
      case Some(column: String)   => writer.partitionBy(column)
      case _                      => writer
    }

    finalWriter.save(s3Path)
    logger.info(s"Loaded data to S3 at $s3Path in $format format")
  }

  /**
   * Loads data to DeltaLake.
   *
   * @param df          DataFrame to load
   * @param config      Configuration including path, mode
   * @param spark       SparkSession
   * @param isStreaming Whether to use streaming write
   * @return Option[StreamingQuery] if streaming, None if batch
   */
  def toDeltaLake(
      df: DataFrame,
      config: Map[String, Any],
      spark: SparkSession,
      isStreaming: Boolean = false,
  ): Option[org.apache.spark.sql.streaming.StreamingQuery] = {
    val mode = if (isStreaming) "STREAMING" else "BATCH"
    logger.info(s"Loading to DeltaLake in $mode mode")

    require(config.contains("path"), "'path' is required")

    val path = config("path").toString

    if (isStreaming) {
      // Streaming write
      logger.info(s"Writing stream to DeltaLake: $path")

      val checkpointLocation = config
        .getOrElse("checkpointLocation", s"/tmp/checkpoints/delta_${java.util.UUID.randomUUID().toString}")
        .toString

      val outputMode      = config.getOrElse("outputMode", "append").toString
      val triggerInterval = config.getOrElse("trigger", "5 seconds").toString

      var streamWriter = df.writeStream
        .format("delta")
        .outputMode(outputMode)
        .option("checkpointLocation", checkpointLocation)

      // Add partitioning if specified
      config.get("partitionBy") match {
        case Some(columns: List[_]) =>
          streamWriter = streamWriter.partitionBy(columns.map(_.toString): _*)
        case Some(column: String)   =>
          streamWriter = streamWriter.partitionBy(column)
        case _                      => // No partitioning
      }

      // Add merge schema option if specified
      config.get("mergeSchema") match {
        case Some(merge: Boolean) =>
          streamWriter = streamWriter.option("mergeSchema", merge.toString)
        case Some(merge: String)  =>
          streamWriter = streamWriter.option("mergeSchema", merge)
        case _                    => // No merge schema
      }

      // Parse trigger
      val trigger = parseTrigger(triggerInterval)

      val query = streamWriter.trigger(trigger).start(path)
      logger.info(s"Started streaming query to DeltaLake: name=${query.name}, id=${query.id}")
      Some(query)

    } else {
      // Batch write
      val saveMode = parseSaveMode(config.getOrElse("mode", "append").toString)

      val writer = df.write.format("delta").mode(saveMode)

      // Add partitioning if specified
      val partitionedWriter = config.get("partitionBy") match {
        case Some(columns: List[_]) => writer.partitionBy(columns.map(_.toString): _*)
        case Some(column: String)   => writer.partitionBy(column)
        case _                      => writer
      }

      // Add merge schema option if specified
      val finalWriter = config.get("mergeSchema") match {
        case Some(merge: Boolean) => partitionedWriter.option("mergeSchema", merge.toString)
        case Some(merge: String)  => partitionedWriter.option("mergeSchema", merge)
        case _                    => partitionedWriter
      }

      // Add overwrite schema option if specified
      val overwriteWriter = config.get("overwriteSchema") match {
        case Some(overwrite: Boolean) => finalWriter.option("overwriteSchema", overwrite.toString)
        case Some(overwrite: String)  => finalWriter.option("overwriteSchema", overwrite)
        case _                        => finalWriter
      }

      overwriteWriter.save(path)
      logger.info(s"Loaded data to DeltaLake at $path")
      None
    }
  }

  private def resolveJdbcCredentials(config: Map[String, Any], credentialType: String): JdbcConfig =
    CredentialResolver.resolveJdbcCredentials(config, credentialType)

  private def resolveS3Credentials(config: Map[String, Any]): IAMConfig =
    CredentialResolver.resolveS3Credentials(config)

  private def resolveKafkaCredentials(config: Map[String, Any]): Map[String, Any] =
    CredentialResolver.resolveKafkaCredentials(config)

  /**
   * Parses SaveMode from string.
   */
  private def parseSaveMode(mode: String): SaveMode = mode.toLowerCase match {
    case "append"        => SaveMode.Append
    case "overwrite"     => SaveMode.Overwrite
    case "errorifexists" => SaveMode.ErrorIfExists
    case "ignore"        => SaveMode.Ignore
    case _               =>
      throw new IllegalArgumentException(
        s"Invalid save mode: $mode. Must be one of: append, overwrite, errorifexists, ignore",
      )
  }

  /**
   * Parses trigger string into Trigger object for streaming queries.
   */
  private def parseTrigger(triggerStr: String): org.apache.spark.sql.streaming.Trigger = {
    import org.apache.spark.sql.streaming.Trigger

    triggerStr.toLowerCase match {
      case "once"                                                                     => Trigger.Once()
      case s if s.startsWith("processingtime")                                        =>
        // Format: "processingTime=5 seconds"
        val duration = s.split("=")(1).trim
        Trigger.ProcessingTime(duration)
      case s if s.contains("seconds") || s.contains("minutes") || s.contains("hours") =>
        // Format: "5 seconds", "1 minute", etc.
        Trigger.ProcessingTime(triggerStr)
      case _                                                                          =>
        logger.warn(s"Unknown trigger format: $triggerStr, defaulting to 5 seconds")
        Trigger.ProcessingTime("5 seconds")
    }
  }

  /**
   * Writes data to Avro files.
   *
   * @param df     DataFrame to write
   * @param config Configuration including path, compression, partitionBy
   * @param spark  SparkSession
   */
  def toAvro(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Loading to Avro")

    require(config.contains("path"), "'path' is required")

    val path = config("path").toString
    AvroConverter.writeAvro(df, path, config)(spark)
  }
}
