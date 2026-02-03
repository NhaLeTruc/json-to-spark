package com.pipeline.operations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import com.pipeline.avro.AvroConverter

import scala.reflect.ClassTag

/**
 * User-facing transformation methods.
 *
 * Implements FR-004: Transform operations.
 * Implements FR-024: Support at least 5 transform methods.
 * Implements FR-025: Support at least 5 validation methods.
 * Validates Constitution Section V: Library-First Architecture.
 */
object UserMethods {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private def getAs[T](config: Map[String, Any], key: String)(implicit ct: ClassTag[T]): T =
    config.get(key) match {
      case Some(value: T) => value
      case Some(other) => throw new IllegalArgumentException(
        s"Config key '$key' expected ${ct.runtimeClass.getSimpleName} but got ${other.getClass.getSimpleName}"
      )
      case None => throw new IllegalArgumentException(s"Config key '$key' is required")
    }

  // ==================== TRANSFORM METHODS ====================

  /**
   * Filters rows based on a SQL condition.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "condition" key
   * @param spark  SparkSession
   * @return Filtered DataFrame
   */
  def filterRows(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Filtering rows")

    require(config.contains("condition"), "'condition' is required")

    val condition = config("condition").toString

    // Validate condition doesn't contain dangerous patterns
    require(!condition.toLowerCase.contains("--"), "SQL comments not allowed in conditions")
    require(!condition.contains(";"), "Multiple statements not allowed")

    df.filter(condition)
  }

  /**
   * Enriches data by adding computed columns.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "columns" map (column_name -> SQL expression)
   * @param spark  SparkSession
   * @return Enriched DataFrame
   */
  def enrichData(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Enriching data")

    require(config.contains("columns"), "'columns' map is required")

    val columns = getAs[Map[String, String]](config, "columns")

    import org.apache.spark.sql.functions.expr

    columns.foldLeft(df) { case (accDf, (columnName, expression)) =>
      accDf.withColumn(columnName, expr(expression))
    }
  }

  /**
   * Joins multiple DataFrames.
   *
   * @param df     Primary DataFrame (not used, uses resolvedDataFrames from config)
   * @param config Configuration with "inputDataFrames" list, "joinType", "joinConditions", and "resolvedDataFrames"
   * @param spark  SparkSession
   * @return Joined DataFrame
   */
  def joinDataFrames(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Joining DataFrames")

    require(config.contains("inputDataFrames"), "'inputDataFrames' list is required")
    require(config.contains("joinConditions"), "'joinConditions' list is required")
    require(config.contains("resolvedDataFrames"), "DataFrames must be resolved by TransformStep")

    val inputDfNames   = getAs[List[String]](config, "inputDataFrames")
    val joinConditions = getAs[List[String]](config, "joinConditions")
    val joinType       = config.getOrElse("joinType", "inner").toString
    val resolvedDFs    = getAs[Map[String, DataFrame]](config, "resolvedDataFrames")

    require(inputDfNames.size >= 2, "At least 2 DataFrames required for join")
    require(joinConditions.size == inputDfNames.size - 1, "Need N-1 join conditions for N DataFrames")

    // Get DataFrames in order and alias them with their names for qualified column references
    val dataFrames = inputDfNames.map { name =>
      val df = resolvedDFs.getOrElse(
        name,
        throw new IllegalStateException(s"DataFrame '$name' not found in resolved DataFrames"),
      )
      df.alias(name) // Alias DataFrame with its registration name
    }

    import org.apache.spark.sql.functions.expr

    // Perform sequential joins
    var result = dataFrames.head
    dataFrames.tail.zip(joinConditions).zip(inputDfNames.tail).foreach { case ((rightDf, condition), rightName) =>
      logger.info(s"Joining with condition: $condition (type: $joinType)")

      // Perform the join first
      val joined = result.join(rightDf, expr(condition), joinType)

      // Extract join keys from condition to identify duplicate columns to drop
      // Parse condition like "users.user_id = orders.user_id" to identify common columns
      val joinKeysPattern = """(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)""".r
      result = condition match {
        case joinKeysPattern(leftAlias, leftKey, rightAlias, rightKey) if leftKey == rightKey =>
          // After join, drop the duplicate key column from the right side to avoid conflicts
          // The joined DataFrame will have both columns, so we need to drop the one from right
          logger.debug(s"Dropping duplicate column from right side ($rightAlias.$rightKey) to avoid conflicts")
          // Drop column using qualified name since both sides have it after join
          joined.drop(rightDf(rightKey))
        case _ =>
          joined
      }
    }

    logger.info(s"Successfully joined ${inputDfNames.size} DataFrames")
    result
  }

  /**
   * Aggregates data by grouping and computing metrics.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "groupBy" columns and "aggregations" map (col -> agg_function)
   * @param spark  SparkSession
   * @return Aggregated DataFrame
   */
  def aggregateData(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Aggregating data")

    require(config.contains("groupBy"), "'groupBy' columns are required")
    require(config.contains("aggregations"), "'aggregations' map is required")

    val groupByCols  = getAs[List[String]](config, "groupBy")
    val aggregations = getAs[Map[String, String]](config, "aggregations")

    import org.apache.spark.sql.functions._

    val grouped = df.groupBy(groupByCols.map(col): _*)

    val aggExprs = aggregations.map { case (columnName, aggFunc) =>
      aggFunc.toLowerCase match {
        case "sum"          => sum(col(columnName)).alias(s"${columnName}_sum")
        case "avg" | "mean" => avg(col(columnName)).alias(s"${columnName}_avg")
        case "count"        => count(col(columnName)).alias(s"${columnName}_count")
        case "min"          => min(col(columnName)).alias(s"${columnName}_min")
        case "max"          => max(col(columnName)).alias(s"${columnName}_max")
        case _              => throw new IllegalArgumentException(s"Unsupported aggregation function: $aggFunc")
      }
    }.toSeq

    grouped.agg(aggExprs.head, aggExprs.tail: _*)
  }

  /**
   * Reshapes data using pivot or unpivot operations.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "operation" (pivot/unpivot), "pivotColumn", etc.
   * @param spark  SparkSession
   * @return Reshaped DataFrame
   */
  def reshapeData(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Reshaping data")

    require(config.contains("operation"), "'operation' (pivot/unpivot) is required")

    val operation = config("operation").toString.toLowerCase

    operation match {
      case "pivot" =>
        require(config.contains("pivotColumn"), "'pivotColumn' is required for pivot")
        require(config.contains("groupByColumns"), "'groupByColumns' is required for pivot")

        val pivotCol    = config("pivotColumn").toString
        val groupByCols = getAs[List[String]](config, "groupByColumns")
        val aggColumn   = config.get("aggregateColumn").map(_.toString)

        import org.apache.spark.sql.functions._

        val grouped = df.groupBy(groupByCols.map(col): _*)
        val pivoted = grouped.pivot(pivotCol)

        aggColumn match {
          case Some(aggCol) => pivoted.agg(sum(col(aggCol)))
          case None         => pivoted.count()
        }

      case "unpivot" =>
        require(config.contains("valueCols"), "'valueCols' is required for unpivot")
        require(config.contains("variableColName"), "'variableColName' is required for unpivot")
        require(config.contains("valueColName"), "'valueColName' is required for unpivot")

        val valueCols       = getAs[List[String]](config, "valueCols")
        val variableColName = config("variableColName").toString
        val valueColName    = config("valueColName").toString
        val idCols          = config
          .get("idCols")
          .map(v => v match {
            case list: List[_] => list.map(_.toString)
            case other => throw new IllegalArgumentException(
              s"Config key 'idCols' expected List but got ${other.getClass.getSimpleName}")
          })
          .getOrElse(
            df.columns.filterNot(valueCols.contains).toList,
          )

        import org.apache.spark.sql.functions._

        // Build stack expression: stack(n, 'col1', col1, 'col2', col2, ...)
        val stackExpr = valueCols.map(col => s"'$col', `$col`").mkString(", ")
        val numCols   = valueCols.length

        // Select id columns plus the unpivoted columns
        val selectCols = idCols.mkString(", ") match {
          case ""  => s"stack($numCols, $stackExpr) as ($variableColName, $valueColName)"
          case ids => s"$ids, stack($numCols, $stackExpr) as ($variableColName, $valueColName)"
        }

        logger.info(s"Unpivoting ${valueCols.size} columns to ($variableColName, $valueColName)")
        df.selectExpr(selectCols.split(", "): _*)

      case _ =>
        throw new IllegalArgumentException(s"Invalid reshape operation: $operation. Must be 'pivot' or 'unpivot'")
    }
  }

  /**
   * Unions multiple DataFrames.
   *
   * @param df     Primary DataFrame (starting point for union)
   * @param config Configuration with "inputDataFrames" list, "distinct" flag, and "resolvedDataFrames"
   * @param spark  SparkSession
   * @return Unioned DataFrame
   */
  def unionDataFrames(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Unioning DataFrames")

    require(config.contains("inputDataFrames"), "'inputDataFrames' list is required")
    require(config.contains("resolvedDataFrames"), "DataFrames must be resolved by TransformStep")

    val inputDfNames = getAs[List[String]](config, "inputDataFrames")
    val distinct     = config.getOrElse("distinct", false).asInstanceOf[Boolean]
    val resolvedDFs  = getAs[Map[String, DataFrame]](config, "resolvedDataFrames")

    // Get DataFrames to union
    val dataFrames = inputDfNames.map { name =>
      resolvedDFs.getOrElse(
        name,
        throw new IllegalStateException(s"DataFrame '$name' not found in resolved DataFrames"),
      )
    }

    // Union all DataFrames
    var result = df
    dataFrames.foreach { nextDf =>
      result = result.union(nextDf)
    }

    // Apply distinct if requested
    val finalResult = if (distinct) {
      logger.info("Applying distinct to unioned DataFrame")
      result.distinct()
    } else {
      result
    }

    logger.info(s"Successfully unioned ${inputDfNames.size + 1} DataFrames (distinct=$distinct)")
    finalResult
  }

  // ==================== VALIDATION METHODS ====================

  /**
   * Validates DataFrame schema.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "expectedColumns" list of maps with name and type
   * @param spark  SparkSession
   */
  def validateSchema(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Validating schema")

    require(config.contains("expectedColumns"), "'expectedColumns' is required")

    val expectedColumns = getAs[List[Map[String, String]]](config, "expectedColumns")
    val actualSchema    = df.schema

    expectedColumns.foreach { colSpec =>
      val colName = colSpec("name")
      val colType = colSpec("type")

      val actualField = actualSchema.fields.find(_.name == colName)
      actualField match {
        case None                                                              =>
          throw new IllegalStateException(s"Schema validation failed: Column '$colName' not found")
        case Some(field) if !field.dataType.typeName.equalsIgnoreCase(colType) =>
          throw new IllegalStateException(
            s"Schema validation failed: Column '$colName' has type '${field.dataType.typeName}' but expected '$colType'",
          )
        case Some(_)                                                           =>
          logger.debug(s"Column '$colName' validation passed")
      }
    }

    logger.info(s"Schema validation passed for ${expectedColumns.size} columns")
  }

  /**
   * Validates null values.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "notNullColumns" list
   * @param spark  SparkSession
   */
  def validateNulls(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Validating nulls")

    require(config.contains("notNullColumns"), "'notNullColumns' list is required")

    val notNullColumns = getAs[List[String]](config, "notNullColumns")

    import org.apache.spark.sql.functions._

    notNullColumns.foreach { colName =>
      val nullCount = df.filter(col(colName).isNull).count()
      if (nullCount > 0) {
        throw new IllegalStateException(
          s"Null validation failed: Column '$colName' has $nullCount null values",
        )
      }
      logger.debug(s"Column '$colName' has no null values")
    }

    logger.info(s"Null validation passed for ${notNullColumns.size} columns")
  }

  /**
   * Validates value ranges.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "ranges" map (column -> min/max map)
   * @param spark  SparkSession
   */
  def validateRanges(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Validating ranges")

    require(config.contains("ranges"), "'ranges' map is required")

    val ranges = getAs[Map[String, Map[String, Any]]](config, "ranges")

    import org.apache.spark.sql.functions._

    ranges.foreach { case (colName, rangeSpec) =>
      val minValue = rangeSpec.get("min")
      val maxValue = rangeSpec.get("max")

      var condition = lit(true)

      minValue.foreach { min =>
        val violationCount = df.filter(col(colName) < lit(min)).count()
        if (violationCount > 0) {
          throw new IllegalStateException(
            s"Range validation failed: Column '$colName' has $violationCount values below minimum $min",
          )
        }
      }

      maxValue.foreach { max =>
        val violationCount = df.filter(col(colName) > lit(max)).count()
        if (violationCount > 0) {
          throw new IllegalStateException(
            s"Range validation failed: Column '$colName' has $violationCount values above maximum $max",
          )
        }
      }

      logger.debug(s"Column '$colName' range validation passed")
    }

    logger.info(s"Range validation passed for ${ranges.size} columns")
  }

  /**
   * Validates referential integrity.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "foreignKey", "referencedDataFrame", "referencedColumn", and "resolvedReferencedDataFrame"
   * @param spark  SparkSession
   */
  def validateReferentialIntegrity(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Validating referential integrity")

    require(config.contains("foreignKey"), "'foreignKey' column is required")
    require(config.contains("referencedDataFrame"), "'referencedDataFrame' is required")
    require(config.contains("referencedColumn"), "'referencedColumn' is required")
    require(config.contains("resolvedReferencedDataFrame"), "Referenced DataFrame must be resolved by ValidateStep")

    val foreignKey       = config("foreignKey").toString
    val referencedDfName = config("referencedDataFrame").toString
    val referencedColumn = config("referencedColumn").toString
    val referencedDf     = config("resolvedReferencedDataFrame").asInstanceOf[DataFrame]

    import org.apache.spark.sql.functions._

    // Find foreign key values that don't exist in referenced column
    val orphanedRecords = df
      .select(col(foreignKey))
      .filter(col(foreignKey).isNotNull) // Only check non-null foreign keys
      .distinct()
      .join(
        referencedDf.select(col(referencedColumn)).distinct(),
        df(foreignKey) === referencedDf(referencedColumn),
        "left_anti", // Find values in left that don't exist in right
      )

    val violationCount = orphanedRecords.count()

    if (violationCount > 0) {
      // Sample some orphaned values for debugging
      val sample = orphanedRecords.take(5).map(_.get(0)).mkString(", ")
      throw new IllegalStateException(
        s"Referential integrity validation failed: $violationCount orphaned records in '$foreignKey' " +
          s"not found in '$referencedDfName.$referencedColumn'. Sample values: $sample",
      )
    }

    logger.info(s"Referential integrity validation passed for '$foreignKey' -> '$referencedDfName.$referencedColumn'")
  }

  /**
   * Validates business rules.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "rules" list of SQL expressions
   * @param spark  SparkSession
   */
  def validateBusinessRules(df: DataFrame, config: Map[String, Any], spark: SparkSession): Unit = {
    logger.info("Validating business rules")

    require(config.contains("rules"), "'rules' list is required")

    val rules = getAs[List[String]](config, "rules")

    rules.zipWithIndex.foreach { case (rule, index) =>
      // Validate rule doesn't contain dangerous patterns
      require(!rule.toLowerCase.contains("--"), s"SQL comments not allowed in rule #${index + 1}")
      require(!rule.contains(";"), s"Multiple statements not allowed in rule #${index + 1}")

      val violationCount = df.filter(s"NOT ($rule)").count()
      if (violationCount > 0) {
        throw new IllegalStateException(
          s"Business rule validation failed: Rule #${index + 1} '$rule' violated by $violationCount rows",
        )
      }
      logger.debug(s"Business rule #${index + 1} passed: $rule")
    }

    logger.info(s"Business rule validation passed for ${rules.size} rules")
  }

  // ==================== AVRO CONVERSION METHODS ====================

  /**
   * Converts DataFrame schema to Avro schema.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "namespace" and "name"
   * @param spark  SparkSession
   * @return DataFrame with avro_schema column containing the schema JSON
   */
  def toAvroSchema(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Converting DataFrame schema to Avro schema")

    val namespace = config.getOrElse("namespace", "com.pipeline").toString
    val name      = config.getOrElse("name", "Record").toString

    val avroSchema = AvroConverter.dataFrameToAvroSchema(df, namespace, name)

    // Return DataFrame with schema as a single-row, single-column result
    import spark.implicits._
    Seq(avroSchema).toDF("avro_schema")
  }

  /**
   * Evolves DataFrame schema to match target Avro schema.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "targetSchema" (Avro JSON)
   * @param spark  SparkSession
   * @return DataFrame with evolved schema
   */
  def evolveAvroSchema(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Evolving DataFrame schema to match target Avro schema")

    require(config.contains("targetSchema"), "'targetSchema' is required")

    val targetSchema = config("targetSchema").toString
    AvroConverter.evolveSchema(df, targetSchema)(spark)
  }

  /**
   * Repartitions DataFrame by number of partitions.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "numPartitions" (Int)
   * @param spark  SparkSession
   * @return Repartitioned DataFrame
   */
  def repartition(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Repartitioning DataFrame")

    require(config.contains("numPartitions"), "'numPartitions' is required")

    val numPartitions = config("numPartitions") match {
      case n: Int    => n
      case n: String => n.toInt
      case n         => n.toString.toInt
    }

    logger.info(s"Repartitioning to $numPartitions partitions")
    df.repartition(numPartitions)
  }

  /**
   * Repartitions DataFrame by column expressions.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "numPartitions" (Int, optional) and "columns" (List[String])
   * @param spark  SparkSession
   * @return Repartitioned DataFrame
   */
  def repartitionByColumns(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Repartitioning DataFrame by columns")

    require(config.contains("columns"), "'columns' is required")

    val columns = config("columns") match {
      case list: List[_] => list.map(_.toString)
      case col: String   => List(col)
      case _             => throw new IllegalArgumentException("'columns' must be a List or String")
    }

    val numPartitions = config.get("numPartitions").map {
      case n: Int    => n
      case n: String => n.toInt
      case n         => n.toString.toInt
    }

    logger.info(
      s"Repartitioning by columns: ${columns.mkString(", ")}" +
        numPartitions.map(n => s" into $n partitions").getOrElse(""),
    )

    numPartitions match {
      case Some(n) => df.repartition(n, columns.map(df(_)): _*)
      case None    => df.repartition(columns.map(df(_)): _*)
    }
  }

  /**
   * Coalesces DataFrame to reduce number of partitions.
   *
   * @param df     Input DataFrame
   * @param config Configuration with "numPartitions" (Int)
   * @param spark  SparkSession
   * @return Coalesced DataFrame
   */
  def coalesce(df: DataFrame, config: Map[String, Any], spark: SparkSession): DataFrame = {
    logger.info("Coalescing DataFrame")

    require(config.contains("numPartitions"), "'numPartitions' is required")

    val numPartitions = config("numPartitions") match {
      case n: Int    => n
      case n: String => n.toInt
      case n         => n.toString.toInt
    }

    logger.info(s"Coalescing to $numPartitions partitions")
    df.coalesce(numPartitions)
  }
}
