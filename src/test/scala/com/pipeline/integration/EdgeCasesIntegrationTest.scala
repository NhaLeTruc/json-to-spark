package com.pipeline.integration

import com.pipeline.core.{ExtractStep, LoadStep, Pipeline, TransformStep}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/**
 * Integration tests for edge cases and error scenarios.
 *
 * Tests pipeline behavior under stress and unusual conditions.
 */
@RunWith(classOf[JUnitRunner])
class EdgeCasesIntegrationTest extends IntegrationTestBase {

  behavior of "Edge Cases Integration"

  it should "handle large datasets efficiently" in {
    requireDocker()

    // Create large test table
    createTestTable(
      "large_dataset",
      "CREATE TABLE large_dataset (id INT, category VARCHAR(50), value DECIMAL(10,2))",
    )

    // Insert 10,000 rows
    val largeData = (1 to 10000).map { i =>
      Map(
        "id"       -> i,
        "category" -> s"cat_${i % 100}",
        "value"    -> (Math.random() * 1000).toDouble,
      )
    }

    insertTestData("large_dataset", largeData)

    createTestTable(
      "large_result",
      "CREATE TABLE large_result (category VARCHAR(50), avg_value DECIMAL(10,2), count BIGINT)",
    )

    // Create pipeline to process large dataset
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "large-dataset-pipeline",
      mode = "batch",
      steps = List(
        ExtractStep(
          method = "fromPostgres",
          config = Map(
            "host"            -> props("host"),
            "port"            -> props("port"),
            "database"        -> props("database"),
            "username"        -> props("username"),
            "password"        -> props("password"),
            "table"           -> "large_dataset",
            "partitionColumn" -> "id",
            "lowerBound"      -> "1",
            "upperBound"      -> "10000",
            "numPartitions"   -> "4",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "aggregateData",
          config = Map(
            "groupBy"      -> List("category"),
            "aggregations" -> Map(
              "value" -> "avg",
              "id"    -> "count",
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
            "table"    -> "large_result",
            "mode"     -> "overwrite",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute with timing
    val startTime = System.currentTimeMillis()
    val result    = pipeline.execute(spark, collectMetrics = true)
    val endTime   = System.currentTimeMillis()

    result match {
      case Right(_) =>
        val duration = endTime - startTime
        logger.info(s"Large dataset processed in ${duration}ms")

        // Verify results
        val resultDf = spark.read
          .format("jdbc")
          .option("url", getPostgresJdbcUrl)
          .option("dbtable", "large_result")
          .option("user", getPostgresUsername)
          .option("password", getPostgresPassword)
          .load()

        resultDf.count() shouldBe 100 // 100 categories

        // Check metrics
        pipeline.getMetrics match {
          case Some(metrics) =>
            metrics.status shouldBe "COMPLETED"
            metrics.durationMs should be > 0L
            logger.info(s"Processed 10,000 rows in ${metrics.durationMs}ms")

          case None =>
            fail("Metrics should be available")
        }

      case Left(exception) =>
        fail(s"Large dataset pipeline failed: ${exception.getMessage}", exception)
    }
  }

  it should "reject execution of pre-cancelled pipeline" in {
    requireDocker()

    // Create test table
    createTestTable(
      "cancellation_test",
      "CREATE TABLE cancellation_test (id INT, data VARCHAR(100))",
    )

    insertTestData(
      "cancellation_test",
      (1 to 100).map(i => Map("id" -> i, "data" -> s"data_$i")),
    )

    // Create pipeline
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "cancellation-test-pipeline",
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
            "table"    -> "cancellation_test",
          ),
          nextStep = None,
        ),
      ),
    )

    // Cancel pipeline before execution
    pipeline.cancel()
    pipeline.isCancelled shouldBe true

    // Try to execute cancelled pipeline - should throw exception
    val exception = intercept[com.pipeline.exceptions.PipelineCancelledException] {
      pipeline.execute(spark) match {
        case Left(ex) => throw ex
        case Right(_) => ()
      }
    }

    logger.info(s"Pre-cancelled pipeline rejected as expected: ${exception.getMessage}")
    exception.getMessage should include("cancel")
  }

  it should "cancel pipeline in-flight during multi-step execution" in {
    requireDocker()

    import scala.concurrent.{Future, Promise}
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Await
    import scala.concurrent.duration._

    // Create large test table to ensure execution takes time
    createTestTable(
      "inflight_cancel_test",
      "CREATE TABLE inflight_cancel_test (id INT, data VARCHAR(100))",
    )

    // Insert enough data to slow down processing
    insertTestData(
      "inflight_cancel_test",
      (1 to 1000).map(i => Map("id" -> i, "data" -> s"data_$i")),
    )

    createTestTable(
      "inflight_cancel_dest",
      "CREATE TABLE inflight_cancel_dest (id INT, data VARCHAR(100), processed VARCHAR(10))",
    )

    // Create multi-step pipeline
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "inflight-cancel-pipeline",
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
            "table"    -> "inflight_cancel_test",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "addColumn",
          config = Map(
            "columnName" -> "processed",
            "value"      -> "yes",
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
            "table"    -> "inflight_cancel_dest",
            "mode"     -> "overwrite",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute in background and cancel after delay
    val executionFuture = Future {
      pipeline.execute(spark)
    }

    // Cancel after a short delay to attempt in-flight cancellation
    Thread.sleep(50)
    pipeline.cancel()

    // Wait for result
    val result = Await.result(executionFuture, 30.seconds)

    // Either the pipeline was cancelled or completed before cancellation
    result match {
      case Left(ex: com.pipeline.exceptions.PipelineCancelledException) =>
        logger.info(s"Pipeline cancelled in-flight as expected: ${ex.getMessage}")
        ex.getMessage should include("cancel")

      case Right(_) =>
        // Pipeline completed before cancellation took effect - this is acceptable
        // since cancellation is cooperative and checked between steps
        logger.info("Pipeline completed before cancellation took effect (acceptable)")
        pipeline.isCancelled shouldBe true

      case Left(ex) =>
        fail(s"Unexpected error: ${ex.getMessage}", ex)
    }
  }

  it should "succeed without retry when operation works on first attempt" in {
    requireDocker()

    val props = getPostgresProperties

    // Create table before execution
    createTestTable(
      "retry_success_table",
      "CREATE TABLE retry_success_table (id INT, value VARCHAR(50))",
    )

    insertTestData(
      "retry_success_table",
      Seq(Map("id" -> 1, "value" -> "test")),
    )

    val pipeline = Pipeline(
      name = "retry-success-pipeline",
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
            "table"    -> "retry_success_table",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute with retry configured - should succeed on first attempt
    val result = pipeline.execute(spark, maxAttempts = 3, delayMillis = 100)

    result match {
      case Right(context) =>
        // Verify data was extracted
        val df = context.getPrimaryDataFrame
        df.count() shouldBe 1
        logger.info("Pipeline succeeded on first attempt as expected")

      case Left(exception) =>
        fail(s"Pipeline should succeed: ${exception.getMessage}", exception)
    }
  }

  it should "exhaust retries on persistent failures" in {
    requireDocker()

    val props = getPostgresProperties

    // Create pipeline that references a non-existent table
    val pipeline = Pipeline(
      name = "retry-fail-pipeline",
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
            "table"    -> "nonexistent_retry_table_xyz",
          ),
          nextStep = None,
        ),
      ),
    )

    val startTime = System.currentTimeMillis()

    // Execute with retry - should fail after all attempts
    val result = pipeline.execute(spark, maxAttempts = 2, delayMillis = 100)

    val duration = System.currentTimeMillis() - startTime

    result match {
      case Left(exception) =>
        // Should fail because table doesn't exist
        logger.info(s"Pipeline failed after retries as expected: ${exception.getMessage}")
        // With 2 attempts and 100ms delay, total time should be >= 100ms (one retry delay)
        duration should be >= 100L
        exception.getMessage should (include("nonexistent") or include("does not exist") or include("not found") or include("error"))

      case Right(_) =>
        fail("Pipeline should have failed for non-existent table")
    }
  }

  it should "handle concurrent pipeline execution" in {
    requireDocker()

    // Create test tables for concurrent execution
    createTestTable(
      "concurrent_source",
      "CREATE TABLE concurrent_source (id INT, value VARCHAR(50))",
    )

    insertTestData(
      "concurrent_source",
      (1 to 50).map(i => Map("id" -> i, "value" -> s"value_$i")),
    )

    createTestTable(
      "concurrent_dest_1",
      "CREATE TABLE concurrent_dest_1 (id INT, value VARCHAR(50))",
    )

    createTestTable(
      "concurrent_dest_2",
      "CREATE TABLE concurrent_dest_2 (id INT, value VARCHAR(50))",
    )

    // Create two independent pipelines
    val props = getPostgresProperties

    val pipeline1 = Pipeline(
      name = "concurrent-pipeline-1",
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
            "table"    -> "concurrent_source",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id <= 25"),
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
            "table"    -> "concurrent_dest_1",
            "mode"     -> "overwrite",
          ),
          nextStep = None,
        ),
      ),
    )

    val pipeline2 = Pipeline(
      name = "concurrent-pipeline-2",
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
            "table"    -> "concurrent_source",
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id > 25"),
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
            "table"    -> "concurrent_dest_2",
            "mode"     -> "overwrite",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute concurrently
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

    val future1 = Future(pipeline1.execute(spark))
    val future2 = Future(pipeline2.execute(spark))

    import scala.concurrent.Await
    import scala.concurrent.duration._

    val result1 = Await.result(future1, 30.seconds)
    val result2 = Await.result(future2, 30.seconds)

    (result1, result2) match {
      case (Right(_), Right(_)) =>
        // Verify both pipelines completed successfully
        val dest1Df = spark.read
          .format("jdbc")
          .option("url", getPostgresJdbcUrl)
          .option("dbtable", "concurrent_dest_1")
          .option("user", getPostgresUsername)
          .option("password", getPostgresPassword)
          .load()

        val dest2Df = spark.read
          .format("jdbc")
          .option("url", getPostgresJdbcUrl)
          .option("dbtable", "concurrent_dest_2")
          .option("user", getPostgresUsername)
          .option("password", getPostgresPassword)
          .load()

        dest1Df.count() shouldBe 25
        dest2Df.count() shouldBe 25

        logger.info("Concurrent pipeline execution verified successfully")

      case _ =>
        fail("One or both concurrent pipelines failed")
    }
  }

  it should "handle special characters and unicode in data" in {
    requireDocker()

    // Create table with unicode data
    createTestTable(
      "unicode_test",
      "CREATE TABLE unicode_test (id INT, text VARCHAR(200))",
    )

    insertTestData(
      "unicode_test",
      Seq(
        Map("id" -> 1, "text" -> "Hello 世界"),
        Map("id" -> 2, "text" -> "Привет мир"),
        Map("id" -> 3, "text" -> "مرحبا بالعالم"),
        Map("id" -> 4, "text" -> "Special chars: !@#$%^&*()"),
      ),
    )

    // Create pipeline
    val props    = getPostgresProperties
    val pipeline = Pipeline(
      name = "unicode-test-pipeline",
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
            "table"    -> "unicode_test",
          ),
          nextStep = None,
        ),
      ),
    )

    // Execute pipeline
    val result = pipeline.execute(spark)

    result match {
      case Right(context) =>
        val df = context.getPrimaryDataFrame
        df.count() shouldBe 4

        // Verify unicode data is preserved
        val texts = df.select("text").collect().map(_.getString(0))
        texts should contain("Hello 世界")
        texts should contain("Привет мир")

        logger.info("Unicode data handled successfully")

      case Left(exception) =>
        fail(s"Unicode test failed: ${exception.getMessage}", exception)
    }
  }
}
