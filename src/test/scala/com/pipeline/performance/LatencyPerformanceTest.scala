package com.pipeline.performance

import com.pipeline.core.{ExtractStep, Pipeline, TransformStep}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/**
 * Latency-focused performance tests.
 *
 * Tests p50, p95, p99 latencies for critical operations
 * to ensure consistent performance and detect outliers.
 *
 * Performance Requirements (from spec.md):
 * - SC-004: Streaming pipelines maintain p95 latency under 5 seconds during steady-state
 * - Constitution: Maximum latency: 5 seconds end-to-end (p95)
 */
@RunWith(classOf[JUnitRunner])
class LatencyPerformanceTest extends PerformanceTestBase {

  behavior of "Latency Performance - Specification Requirements"

  it should "meet SC-004: p95 latency under 5 seconds for streaming operations" in {
    // SC-004: Streaming pipelines maintain p95 latency under 5 seconds during steady-state operation
    val maxP95Latency = 5000L // 5 seconds in milliseconds
    val microBatchSize = 50000 // 50K records per micro-batch
    val iterations = 20 // Multiple micro-batches to measure steady-state

    logger.info(s"Testing SC-004: Streaming p95 latency < ${maxP95Latency}ms")
    logger.info(s"Micro-batch size: $microBatchSize records, Iterations: $iterations")

    val df = createLargeTestDataFrame(microBatchSize)

    // Warmup runs to reach steady-state (eliminate JVM/Spark cold start)
    logger.info("Warmup runs (3 iterations)...")
    (1 to 3).foreach { _ =>
      df.filter("id % 2 = 0").withColumn("is_high", org.apache.spark.sql.functions.expr("value1 > 500")).count()
    }

    // Measure steady-state latency across multiple micro-batches
    logger.info(s"Measuring steady-state latency across $iterations micro-batches...")
    (1 to iterations).foreach { i =>
      benchmark(s"sc004-microbatch-$i") {
        df.filter("id % 2 = 0").withColumn("is_high", org.apache.spark.sql.functions.expr("value1 > 500")).count()
      }
    }

    // Calculate percentiles from collected metrics
    val metrics = performanceMetrics.filter(_._1.startsWith("sc004-microbatch-"))
    val durations = metrics.values.flatMap(_.getMeasurements).toSeq.sorted

    val p50 = percentile(durations, 50)
    val p95 = percentile(durations, 95)
    val p99 = percentile(durations, 99)

    logger.info("Streaming Latency Percentiles (steady-state):")
    logger.info(s"  p50: ${p50}ms")
    logger.info(s"  p95: ${p95}ms (requirement: < ${maxP95Latency}ms)")
    logger.info(s"  p99: ${p99}ms")

    // Assert SC-004 requirement
    p95 should be < maxP95Latency

    logger.info("✓ SC-004 PASSED: Streaming p95 latency under 5 seconds")
  }

  behavior of "Latency Performance - Operational Characteristics"

  it should "measure p50/p95/p99 latencies for pipeline execution" in {
    val df = createLargeTestDataFrame(50000)

    // Run multiple iterations to collect latency distribution
    val iterations = 10
    logger.info(s"Running $iterations iterations to measure latency percentiles...")

    (1 to iterations).foreach { i =>
      benchmark(s"pipeline-latency-iter-$i") {
        df.filter("id % 2 = 0").count()
      }
    }

    // Calculate percentiles from collected metrics
    val metrics = performanceMetrics.filter(_._1.startsWith("pipeline-latency-iter-"))
    metrics should not be empty

    val durations = metrics.values.flatMap(_.getMeasurements).toSeq.sorted
    val p50 = percentile(durations, 50)
    val p95 = percentile(durations, 95)
    val p99 = percentile(durations, 99)

    logger.info(s"Latency Percentiles:")
    logger.info(s"  p50: ${p50}ms")
    logger.info(s"  p95: ${p95}ms")
    logger.info(s"  p99: ${p99}ms")

    // Assert reasonable latency bounds
    p50 should be < 3000L // p50 should be under 3s
    p95 should be < 5000L // p95 should be under 5s
    p99 should be < 10000L // p99 should be under 10s
  }

  it should "measure filter operation latency distribution" in {
    val df = createLargeTestDataFrame(100000)

    val iterations = 20

    (1 to iterations).foreach { i =>
      benchmark(s"filter-latency-$i") {
        df.filter("id % 10 = 0").count()
      }
    }

    val metrics = performanceMetrics.filter(_._1.startsWith("filter-latency-"))
    val durations = metrics.values.flatMap(_.getMeasurements).toSeq.sorted

    val p50 = percentile(durations, 50)
    val p95 = percentile(durations, 95)

    logger.info(s"Filter Operation Latency:")
    logger.info(s"  p50: ${p50}ms")
    logger.info(s"  p95: ${p95}ms")

    // p95 should be within 1.5x of p50 for consistent filter performance
    // Filter is a simple operation and should have low variance
    val p95_to_p50_ratio = if (p50 > 0) p95.toDouble / p50 else 1.0
    logger.info(f"  p95/p50 ratio: $p95_to_p50_ratio%.2f")

    p95_to_p50_ratio should be < 1.5
  }

  it should "measure aggregation latency with consistent p95" in {
    val df = createLargeTestDataFrame(200000)

    val iterations = 15

    (1 to iterations).foreach { i =>
      benchmark(s"agg-latency-$i") {
        df.groupBy("category")
          .count()
          .collect()
      }
    }

    val metrics = performanceMetrics.filter(_._1.startsWith("agg-latency-"))
    val durations = metrics.values.flatMap(_.getMeasurements).toSeq.sorted

    val p50 = percentile(durations, 50)
    val p95 = percentile(durations, 95)
    val p99 = percentile(durations, 99)

    logger.info(s"Aggregation Latency:")
    logger.info(s"  p50: ${p50}ms")
    logger.info(s"  p95: ${p95}ms")
    logger.info(s"  p99: ${p99}ms")

    // Verify stable performance (p95 not too far from p50)
    val p95_to_p50_ratio = p95.toDouble / p50
    logger.info(f"  p95/p50 ratio: $p95_to_p50_ratio%.2f")

    p95_to_p50_ratio should be < 3.0 // p95 should be less than 3x p50
  }

  it should "measure transform chain latency consistency" in {
    val df = createLargeTestDataFrame(100000)

    val iterations = 10

    (1 to iterations).foreach { i =>
      benchmark(s"transform-chain-$i") {
        df.filter("id % 5 = 0")
          .withColumn("is_high", org.apache.spark.sql.functions.expr("value1 > 500"))
          .filter("is_high = true")
          .count()
      }
    }

    val metrics = performanceMetrics.filter(_._1.startsWith("transform-chain-"))
    val durations = metrics.values.flatMap(_.getMeasurements).toSeq.sorted

    val p50 = percentile(durations, 50)
    val p95 = percentile(durations, 95)

    logger.info(s"Transform Chain Latency:")
    logger.info(s"  p50: ${p50}ms")
    logger.info(s"  p95: ${p95}ms")

    // All measurements should be relatively consistent
    durations.foreach { duration =>
      duration should be < (p50 * 3) // No outliers beyond 3x p50
    }
  }

  /**
   * Helper to calculate percentile from sorted sequence.
   * Uses nearest-rank method (standard percentile calculation).
   *
   * @param sorted Sorted sequence of measurements
   * @param p Percentile (0-100)
   * @return Value at the pth percentile
   */
  private def percentile(sorted: Seq[Long], p: Int): Long = {
    if (sorted.isEmpty) return 0L
    if (sorted.size == 1) return sorted.head

    // Nearest-rank method: index = ceil(p/100 * n) - 1
    // Alternative: linear interpolation between ranks
    val index = math.ceil((p / 100.0) * sorted.size).toInt - 1
    sorted(math.max(0, math.min(index, sorted.size - 1)))
  }
}