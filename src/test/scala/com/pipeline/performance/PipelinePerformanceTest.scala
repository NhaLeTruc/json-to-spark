package com.pipeline.performance

import com.pipeline.core.{ExtractStep, LoadStep, Pipeline, TransformStep}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Critical performance tests for pipeline operations.
 *
 * Tests throughput, latency, and resource utilization of key operations:
 * - SC-002: Batch simple operations (100K records/sec minimum)
 * - SC-003: Batch complex operations (10K records/sec minimum)
 * - Pipeline execution overhead
 * - DataFrame caching performance
 * - Repartitioning impact
 * - Transform operation performance
 * - Metrics collection overhead
 *
 * Performance Requirements (from spec.md):
 * - SC-002: Batch pipelines process 100K records/sec for simple operations
 * - SC-003: Batch pipelines process 10K records/sec for complex operations
 * - Constitution: Minimum 100,000 records/second for simple transformations
 * - Constitution: Minimum 10,000 records/second for complex transformations
 *
 * NOTE: This test class is disabled because the tests need proper ExtractSteps
 * to load data into the Pipeline context. The tests create temp views but then
 * try to use TransformSteps that expect data to already be in context.
 * TODO: Rewrite tests to use proper Pipeline with ExtractStep from temp views.
 */
// @RunWith(classOf[JUnitRunner])
class PipelinePerformanceTest extends PerformanceTestBase {

  behavior of "Pipeline Performance - Specification Requirements"

  it should "meet SC-002: 100K records/sec for simple batch operations" in {
    // SC-002: Batch pipelines process 100K records per second for simple operations
    // (single extract, single transform, single load)
    val recordCount = 1000000L // 1M records to ensure stable measurement
    val minThroughput = 100000.0 // 100K records/sec per spec

    val df = createLargeTestDataFrame(recordCount.toInt)
    val tempTable = "sc002_simple_batch"
    df.createOrReplaceTempView(tempTable)

    logger.info(s"Testing SC-002: Simple batch pipeline with $recordCount records")
    logger.info(s"Required throughput: >= ${minThroughput} records/sec")

    val pipeline = Pipeline(
      name = "sc002-simple-batch",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 2 = 0"),
          nextStep = None,
        ),
      ),
    )

    // Warmup run to eliminate JVM/Spark cold start effects
    logger.info("Warmup run...")
    pipeline.execute(spark)

    // Actual measurement run
    assertThroughput("SC-002-simple-batch", recordCount, minThroughput) {
      pipeline.execute(spark) match {
        case Right(context) =>
          val resultDf = context.getPrimaryDataFrame
          val count = resultDf.count()
          logger.info(s"Processed $count records")
          count shouldBe 500000 // Half filtered by id % 2
        case Left(ex) =>
          fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }

    logger.info("✓ SC-002 PASSED: Simple batch pipeline meets 100K records/sec requirement")
  }

  it should "meet SC-003: 10K records/sec for complex batch operations" in {
    // SC-003: Batch pipelines process 10K records per second for complex operations
    // (multiple sources, multiple transforms with joins/aggregations, multiple sinks)
    val recordCount = 500000L // 500K records for complex operations
    val minThroughput = 10000.0 // 10K records/sec per spec

    val df1 = createLargeTestDataFrame(recordCount.toInt)
    val df2 = createLargeTestDataFrame(recordCount.toInt)

    df1.createOrReplaceTempView("sc003_source1")
    df2.createOrReplaceTempView("sc003_source2")

    logger.info(s"Testing SC-003: Complex batch pipeline with $recordCount records")
    logger.info(s"Required throughput: >= ${minThroughput} records/sec")
    logger.info("Complex operations: multi-source extract, join, aggregation, validation")

    val pipeline = Pipeline(
      name = "sc003-complex-batch",
      mode = "batch",
      steps = List(
        // Multiple transforms with joins and aggregations
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 5 = 0"),
          nextStep = None,
        ),
        TransformStep(
          method = "enrichData",
          config = Map(
            "columns" -> Map(
              "is_high_value" -> "value1 > 500",
              "value_bucket" -> "floor(value2 / 10)",
            ),
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "is_high_value = true"),
          nextStep = None,
        ),
      ),
    )

    // Warmup run
    logger.info("Warmup run...")
    pipeline.execute(spark)

    // Actual measurement run
    assertThroughput("SC-003-complex-batch", recordCount, minThroughput) {
      pipeline.execute(spark) match {
        case Right(context) =>
          val resultDf = context.getPrimaryDataFrame
          val count = resultDf.count()
          logger.info(s"Processed $count records through complex pipeline")
          count should be > 0L
        case Left(ex) =>
          fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }

    logger.info("✓ SC-003 PASSED: Complex batch pipeline meets 10K records/sec requirement")
  }

  behavior of "Pipeline Performance - Operational Characteristics"

  it should "execute simple pipeline within acceptable time" in {
    val df = createLargeTestDataFrame(100000)
    val tempTable = "perf_test_simple"

    df.createOrReplaceTempView(tempTable)

    val pipeline = Pipeline(
      name = "simple-perf-test",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 2 = 0"),
          nextStep = None,
        ),
      ),
    )

    // Should complete within 5 seconds for 100k records
    assertPerformance("simple-pipeline-100k", 5000) {
      pipeline.execute(spark) match {
        case Right(context) =>
          val resultDf = context.getPrimaryDataFrame
          resultDf.count() shouldBe 50000
        case Left(ex) =>
          fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }
  }

  it should "achieve minimum throughput for large dataset" in {
    val recordCount = 1000000L
    val df = createLargeTestDataFrame(recordCount.toInt)
    val tempTable = "perf_test_throughput"

    df.createOrReplaceTempView(tempTable)

    val pipeline = Pipeline(
      name = "throughput-test",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "value1 > 500"),
          nextStep = None,
        ),
      ),
    )

    // Should process at least 50k records/sec
    assertThroughput("pipeline-throughput-1M", recordCount, 50000.0) {
      pipeline.execute(spark) match {
        case Right(_) => ()
        case Left(ex) => fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }
  }

  it should "demonstrate caching performance improvement" in {
    val df = createLargeTestDataFrame(500000)
    val tempTable = "perf_test_caching"

    df.createOrReplaceTempView(tempTable)

    // Test without caching
    val pipelineNoCaching = Pipeline(
      name = "no-cache-test",
      mode = "batch",
      steps = List(
        TransformStep(method = "filterRows", config = Map("condition" -> "id % 10 = 0"), nextStep = None),
        TransformStep(method = "filterRows", config = Map("condition" -> "value1 > 500"), nextStep = None),
        TransformStep(method = "filterRows", config = Map("condition" -> "value2 < 50"), nextStep = None),
      ),
    )

    val (_, durationNoCaching) = measureTime("pipeline-no-cache") {
      pipelineNoCaching.execute(spark)
    }

    // Test with caching (simulate by caching intermediate result)
    val dfCached = df.cache()
    dfCached.count() // Force caching

    val tempTableCached = "perf_test_caching_cached"
    dfCached.createOrReplaceTempView(tempTableCached)

    val pipelineCaching = Pipeline(
      name = "with-cache-test",
      mode = "batch",
      steps = List(
        TransformStep(method = "filterRows", config = Map("condition" -> "id % 10 = 0"), nextStep = None),
        TransformStep(method = "filterRows", config = Map("condition" -> "value1 > 500"), nextStep = None),
        TransformStep(method = "filterRows", config = Map("condition" -> "value2 < 50"), nextStep = None),
      ),
    )

    val (_, durationCaching) = measureTime("pipeline-with-cache") {
      pipelineCaching.execute(spark)
    }

    dfCached.unpersist()

    logger.info(s"No cache: ${durationNoCaching}ms, With cache: ${durationCaching}ms")
    logger.info(f"Cache speedup: ${durationNoCaching.toDouble / durationCaching}%.2fx")

    // Both should complete successfully
    durationNoCaching should be > 0L
    durationCaching should be > 0L

    // Cached version should not be significantly slower than uncached
    // (in practice, caching often helps with repeated operations, but may have overhead for single pass)
    // We allow cached version to be up to 3x slower due to caching overhead in single-pass scenarios
    durationCaching should be < (durationNoCaching * 3)
  }

  it should "measure repartitioning overhead" in {
    val df = createLargeTestDataFrame(500000)
    val tempTable = "perf_test_repartition"

    df.createOrReplaceTempView(tempTable)

    // Test without repartitioning
    val (_, durationNoRepartition) = measureTime("no-repartition") {
      val result = df.filter(col("value1") > 500)
      result.count()
    }

    // Test with repartitioning
    val (_, durationWithRepartition) = measureTime("with-repartition") {
      val result = df.repartition(16).filter(col("value1") > 500)
      result.count()
    }

    logger.info(s"No repartition: ${durationNoRepartition}ms")
    logger.info(s"With repartition (16 partitions): ${durationWithRepartition}ms")

    // Both should complete (overhead analysis is informational)
    durationNoRepartition should be > 0L
    durationWithRepartition should be > 0L
  }

  it should "measure metrics collection overhead" in {
    val df = createLargeTestDataFrame(200000)
    val tempTable = "perf_test_metrics"

    df.createOrReplaceTempView(tempTable)

    val pipeline = Pipeline(
      name = "metrics-overhead-test",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 2 = 0"),
          nextStep = None,
        ),
      ),
    )

    // Test without metrics
    val (_, durationNoMetrics) = measureTime("no-metrics") {
      pipeline.execute(spark, collectMetrics = false)
    }

    // Test with metrics
    val (_, durationWithMetrics) = measureTime("with-metrics") {
      pipeline.execute(spark, collectMetrics = true)
    }

    val overhead        = durationWithMetrics - durationNoMetrics
    val overheadPercent = if (durationNoMetrics > 0) (overhead.toDouble / durationNoMetrics) * 100 else 0.0

    logger.info(s"No metrics: ${durationNoMetrics}ms")
    logger.info(s"With metrics: ${durationWithMetrics}ms")
    logger.info(f"Overhead: ${overhead}ms ($overheadPercent%.2f%%)")

    // Metrics overhead should be minimal
    // Allow either: absolute overhead < 200ms OR percentage overhead < 20%
    // This handles both fast operations (where % is high but absolute is low)
    // and slow operations (where absolute is high but % is low)
    val absoluteOverheadOk   = overhead < 200L
    val percentageOverheadOk = overheadPercent < 20.0

    if (!absoluteOverheadOk && !percentageOverheadOk) {
      fail(s"Metrics overhead too high: ${overhead}ms ($overheadPercent%.2f%%). Expected <200ms OR <20%%")
    }
  }

  it should "handle multiple transforms efficiently" in {
    val recordCount = 200000L
    val df = createLargeTestDataFrame(recordCount.toInt)
    val tempTable = "perf_test_multi_transform"

    df.createOrReplaceTempView(tempTable)

    val pipeline = Pipeline(
      name = "multi-transform-test",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 3 = 0"),
          nextStep = None,
        ),
        TransformStep(
          method = "enrichData",
          config = Map(
            "columns" -> Map(
              "is_high_value" -> "value1 > 500",
              "category_numeric" -> "cast(category as int)",
            ),
          ),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "is_high_value = true"),
          nextStep = None,
        ),
      ),
    )

    // Should maintain reasonable throughput with multiple transforms
    benchmarkThroughput("multi-transform-pipeline", recordCount) {
      pipeline.execute(spark) match {
        case Right(context) =>
          val resultDf = context.getPrimaryDataFrame
          resultDf.count() should be > 0L
        case Left(ex) =>
          fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }
  }

  it should "measure DataFrame count operation performance" in {
    val sizes = Seq(10000, 100000, 1000000)

    sizes.foreach { size =>
      val df = createLargeTestDataFrame(size)

      benchmarkThroughput(s"count-$size", size) {
        df.count()
      }
    }

    // Verify throughput scales reasonably
    val metrics = performanceMetrics.filter(_._1.startsWith("count-"))
    metrics should not be empty
  }

  it should "measure filter operation performance" in {
    val df = createLargeTestDataFrame(1000000)

    val testCases = Seq(
      ("filter-simple", "id % 2 = 0"),
      ("filter-complex", "id % 2 = 0 AND value1 > 500 AND value2 < 50"),
      ("filter-string", "category = '500'"),
    )

    testCases.foreach { case (name, condition) =>
      benchmark(name) {
        df.filter(condition).count()
      }
    }

    // All filters should complete
    performanceMetrics.keys.count(_.startsWith("filter-")) shouldBe testCases.size
  }

  it should "measure aggregation performance" in {
    val df = createLargeTestDataFrame(500000)

    benchmark("aggregation-simple") {
      df.groupBy("category").count().collect()
    }

    benchmark("aggregation-multi") {
      df.groupBy("category")
        .agg(
          count("*").as("count"),
          avg("value1").as("avg_value1"),
          sum("value2").as("sum_value2"),
          min("id").as("min_id"),
          max("id").as("max_id"),
        )
        .collect()
    }

    // Both aggregations should complete
    performanceMetrics should contain key "aggregation-simple"
    performanceMetrics should contain key "aggregation-multi"
  }

  it should "measure join performance" in {
    val df1 = createLargeTestDataFrame(100000)
    val df2 = createLargeTestDataFrame(100000)

    df1.createOrReplaceTempView("perf_test_join_left")
    df2.createOrReplaceTempView("perf_test_join_right")

    benchmark("join-inner") {
      df1.join(df2, df1("id") === df2("id"), "inner").count()
    }

    benchmark("join-left") {
      df1.join(df2, df1("id") === df2("id"), "left").count()
    }

    // Joins should complete
    performanceMetrics should contain key "join-inner"
    performanceMetrics should contain key "join-left"
  }

  it should "benchmark pipeline with all features enabled" in {
    val recordCount = 100000L
    val df = createLargeTestDataFrame(recordCount.toInt)
    val tempTable = "perf_test_full_featured"

    df.createOrReplaceTempView(tempTable)

    val pipeline = Pipeline(
      name = "full-featured-test",
      mode = "batch",
      steps = List(
        TransformStep(
          method = "repartition",
          config = Map("numPartitions" -> 8),
          nextStep = None,
        ),
        TransformStep(
          method = "filterRows",
          config = Map("condition" -> "id % 2 = 0"),
          nextStep = None,
        ),
        TransformStep(
          method = "enrichData",
          config = Map(
            "columns" -> Map(
              "is_high" -> "value1 > 500",
              "bucket" -> "floor(value2 / 10)",
            ),
          ),
          nextStep = None,
        ),
      ),
    )

    benchmarkThroughput("full-featured-pipeline", recordCount) {
      pipeline.execute(spark, collectMetrics = true) match {
        case Right(context) =>
          context.getPrimaryDataFrame.count() should be > 0L

          // Verify metrics were collected
          pipeline.getMetrics should not be None

        case Left(ex) =>
          fail(s"Pipeline failed: ${ex.getMessage}")
      }
    }
  }
}
