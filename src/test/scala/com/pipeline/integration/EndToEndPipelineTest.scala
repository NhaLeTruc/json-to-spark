package com.pipeline.integration

import com.pipeline.config.PipelineConfigParser
import com.pipeline.core.{ExtractStep, LoadStep, Pipeline, TransformStep}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/**
 * End-to-end integration tests for complete pipeline execution.
 *
 * Tests the full pipeline workflow from extract to load with real containers.
 * Implements Sprint 1-2 Task 1.2: Integration Testing Suite.
 */
@RunWith(classOf[JUnitRunner])
class EndToEndPipelineTest extends IntegrationTestBase {

  behavior of "End-to-End Pipeline"

  it should "execute a complete Postgres to Postgres pipeline" in {
    requireDocker()

    // Create source table with test data
    createTestTable(
      "users_source",
      """
        |CREATE TABLE users_source (
        |  id SERIAL PRIMARY KEY,
        |  name VARCHAR(100),
        |  email VARCHAR(100),
        |  age INT,
        |  status VARCHAR(20),
        |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        |)
      """.stripMargin,
    )

    // Insert test data
    insertTestData(
      "users_source",
      Seq(
        Map("id" -> 1, "name" -> "Alice", "email"   -> "alice@example.com", "age"   -> 30, "status" -> "active"),
        Map("id" -> 2, "name" -> "Bob", "email"     -> "bob@example.com", "age"     -> 25, "status" -> "active"),
        Map("id" -> 3, "name" -> "Charlie", "email" -> "charlie@example.com", "age" -> 35, "status" -> "inactive"),
        Map("id" -> 4, "name" -> "Diana", "email"   -> "diana@example.com", "age"   -> 28, "status" -> "active"),
      ),
    )

    // Create destination table
    createTestTable(
      "users_processed",
      """
        |CREATE TABLE users_processed (
        |  id INT,
        |  name VARCHAR(100),
        |  email VARCHAR(100),
        |  age INT,
        |  status VARCHAR(20),
        |  is_adult BOOLEAN
        |)
      """.stripMargin,
    )

    // Store credentials in Vault
    storeVaultSecret(
      "secret/data/postgres",
      getPostgresProperties,
    )

    // Create pipeline
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "e2e-test-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"     -> props("host"),
            "port"     -> props("port"),
            "database" -> props("database"),
            "username" -> props("username"),
            "password" -> props("password"),
            "table"    -> "users_source",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map(
            "condition" -> "status = 'active'",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "enrichData",
          config = Map(
            "columns" -> Map(
              "is_adult" -> "age >= 18",
            ),
          ),
          nextStep = None,
        ),
        LoadStep(
          method = "toPostgres",
          config = Map(
            "host"     -> props("host"),
            "port"     -> props("port"),
            "database" -> props("database"),
            "username" -> props("username"),
            "password" -> props("password"),
            "table"    -> "users_processed",
            "mode"     -> "overwrite",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute pipeline
    val result = pipeline.execute(spark)

    result match {
      case Right(context) =>
        logger.info("Pipeline executed successfully")

        // Verify results
        val resultDf = spark.read
          .format("jdbc")
          .option("url", getPostgresJdbcUrl)
          .option("dbtable", "users_processed")
          .option("user", getPostgresUsername)
          .option("password", getPostgresPassword)
          .load()

        resultDf.count() shouldBe 3                          // Only active users
        resultDf.filter(col("age") >= 18).count() shouldBe 3 // All adults
        resultDf.filter(col("is_adult") === true).count() shouldBe 3

        logger.info(s"Verified: ${resultDf.count()} processed records")

      case Left(exception) =>
        fail(s"Pipeline execution failed: ${exception.getMessage}", exception)
    }
  }

  it should "handle pipeline failures gracefully" in {
    requireDocker()

    // Create pipeline with invalid configuration
    val pipeline = Pipeline(
      name = "failing-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"     -> "invalid-host",
            "port"     -> "5432",
            "database" -> "testdb",
            "username" -> "user",
            "password" -> "pass",
            "table"    -> "nonexistent_table",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute pipeline (should fail)
    val result = pipeline.execute(spark, maxAttempts = 2, delayMillis = 100)

    result match {
      case Left(exception) =>
        logger.info(s"Pipeline failed as expected: ${exception.getMessage}")
        exception.getMessage should include("connection")

      case Right(_) =>
        fail("Pipeline should have failed but succeeded")
    }
  }

  it should "collect metrics during pipeline execution" in {
    requireDocker()

    // Create simple test table
    createTestTable(
      "metrics_test",
      "CREATE TABLE metrics_test (id INT, value VARCHAR(50))",
    )

    insertTestData(
      "metrics_test",
      Seq(
        Map("id" -> 1, "value" -> "test1"),
        Map("id" -> 2, "value" -> "test2"),
      ),
    )

    // Create pipeline
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "metrics-test-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"     -> props("host"),
            "port"     -> props("port"),
            "database" -> props("database"),
            "username" -> props("username"),
            "password" -> props("password"),
            "table"    -> "metrics_test",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute with metrics collection
    val result = pipeline.execute(spark, collectMetrics = true)

    result match {
      case Right(_) =>
        // Check metrics
        pipeline.getMetrics match {
          case Some(metrics) =>
            metrics.pipelineName shouldBe "metrics-test-pipeline"
            metrics.mode shouldBe "batch"
            metrics.status shouldBe "COMPLETED"
            metrics.durationMs should be > 0L
            metrics.getStepMetrics should not be empty
            metrics.getStepMetrics.head.stepType shouldBe "extract"

            logger.info(s"Metrics collected: duration=${metrics.durationMs}ms, steps=${metrics.getStepMetrics.size}")

          case None =>
            fail("Metrics should have been collected")
        }

      case Left(exception) =>
        fail(s"Pipeline execution failed: ${exception.getMessage}", exception)
    }
  }

  it should "execute pipeline with caching enabled" in {
    requireDocker()

    // Create test table
    createTestTable(
      "cache_test",
      "CREATE TABLE cache_test (id INT, data VARCHAR(100))",
    )

    insertTestData(
      "cache_test",
      (1 to 100).map(i => Map("id" -> i, "data" -> s"data_$i")),
    )

    // Create pipeline with caching
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "cache-test-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"              -> props("host"),
            "port"              -> props("port"),
            "database"          -> props("database"),
            "username"          -> props("username"),
            "password"          -> props("password"),
            "table"             -> "cache_test",
            "cache"             -> true,
            "cacheStorageLevel" -> "MEMORY_ONLY",
          ),
          nextStep = None,
        ),
      ),
    )

    val result = pipeline.execute(spark)

    result match {
      case Right(context) =>
        // Verify caching
        context.isCached("__primary__") shouldBe true
        context.cachedNames should contain("__primary__")

        logger.info("Caching verified successfully")

      case Left(exception) =>
        fail(s"Pipeline execution failed: ${exception.getMessage}", exception)
    }
  }

  it should "execute pipeline with repartitioning" in {
    requireDocker()

    // Create test table
    createTestTable(
      "repartition_test",
      "CREATE TABLE repartition_test (id INT, category VARCHAR(50))",
    )

    insertTestData(
      "repartition_test",
      (1 to 50).map(i => Map("id" -> i, "category" -> s"cat_${i % 5}")),
    )

    // Create pipeline with repartitioning
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "repartition-test-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"     -> props("host"),
            "port"     -> props("port"),
            "database" -> props("database"),
            "username" -> props("username"),
            "password" -> props("password"),
            "table"    -> "repartition_test",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "repartition",
          config = Map(
            "numPartitions" -> 8,
          ),
          nextStep = None,
        ),
      ),
    )

    val result = pipeline.execute(spark)

    result match {
      case Right(context) =>
        val df = context.getPrimaryDataFrame
        df.rdd.getNumPartitions shouldBe 8

        logger.info(s"Repartitioning verified: ${df.rdd.getNumPartitions} partitions")

      case Left(exception) =>
        fail(s"Pipeline execution failed: ${exception.getMessage}", exception)
    }
  }
}
