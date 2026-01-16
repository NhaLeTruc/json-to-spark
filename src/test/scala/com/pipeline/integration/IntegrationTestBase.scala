package com.pipeline.integration

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.{GenericContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName

import java.net.Socket
import scala.util.{Try, Using}

/**
 * Base class for integration tests with Testcontainers support.
 *
 * Implements Sprint 1-2 Task 1.2: Integration Testing Suite.
 * Provides shared infrastructure for E2E pipeline tests.
 *
 * First attempts to use docker-compose services, then falls back to Testcontainers.
 */
trait IntegrationTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  // Spark session for tests
  protected var spark: SparkSession = _

  // Testcontainers (used as fallback)
  protected var postgresContainer: PostgreSQLContainer[_] = _
  protected var vaultContainer: GenericContainer[_]       = _

  // Docker-compose service configuration
  protected var useDockerCompose: Boolean              = false
  protected var dockerComposePostgresHost: String      = "localhost"
  protected var dockerComposePostgresPort: Int         = 5432
  protected var dockerComposePostgresDb: String        = "pipelinedb"
  protected var dockerComposePostgresUser: String      = "pipelineuser"
  protected var dockerComposePostgresPassword: String  = "pipelinepass"
  protected var dockerComposeVaultHost: String         = "localhost"
  protected var dockerComposeVaultPort: Int            = 8200

  /**
   * Checks if a service is running on the given host and port.
   */
  private def isServiceRunning(host: String, port: Int): Boolean =
    Try {
      Using.resource(new Socket(host, port)) { socket =>
        socket.isConnected
      }
    }.getOrElse(false)

  /**
   * Sets up Spark and containers before all tests.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    logger.info("Setting up integration test environment...")

    // Create SparkSession
    spark = SparkSession
      .builder()
      .appName("IntegrationTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created")

    // First, check if docker-compose services are running
    if (isServiceRunning(dockerComposePostgresHost, dockerComposePostgresPort)) {
      logger.info("Docker-compose PostgreSQL service detected, using it for tests")
      useDockerCompose = true
    } else {
      logger.info("Docker-compose services not detected, attempting to start Testcontainers")
      // Start Postgres container
      startPostgresContainer()
    }

    // Check Vault
    if (isServiceRunning(dockerComposeVaultHost, dockerComposeVaultPort)) {
      logger.info("Docker-compose Vault service detected, using it for tests")
      System.setProperty("VAULT_ADDR", s"http://$dockerComposeVaultHost:$dockerComposeVaultPort")
      System.setProperty("VAULT_TOKEN", "dev-token")
    } else if (!useDockerCompose) {
      // Start Vault container only if not using docker-compose
      startVaultContainer()
    }

    logger.info("Integration test environment ready")
  }

  /**
   * Cleans up Spark and containers after all tests.
   */
  override def afterAll(): Unit = {
    logger.info("Tearing down integration test environment...")

    // Stop SparkSession
    if (spark != null) {
      spark.stop()
      logger.info("SparkSession stopped")
    }

    // Stop containers
    stopPostgresContainer()
    stopVaultContainer()

    super.afterAll()
    logger.info("Integration test environment cleaned up")
  }

  /**
   * Cleans up test data before each test.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTestData()
  }

  /**
   * Starts PostgreSQL container for testing.
   */
  private def startPostgresContainer(): Unit =
    try {
      logger.info("Starting PostgreSQL container...")
      postgresContainer = new PostgreSQLContainer(
        DockerImageName.parse("postgres:15-alpine"),
      )
      postgresContainer.withDatabaseName("testdb")
      postgresContainer.withUsername("testuser")
      postgresContainer.withPassword("testpass")
      postgresContainer.start()

      logger.info(s"PostgreSQL container started: ${postgresContainer.getJdbcUrl}")
    } catch {
      case ex: Exception =>
        logger.warn(s"Could not start PostgreSQL container: ${ex.getMessage}")
        logger.warn("Integration tests requiring PostgreSQL will be skipped")
    }

  /**
   * Starts Vault container for testing.
   */
  private def startVaultContainer(): Unit =
    try {
      logger.info("Starting Vault container...")
      vaultContainer = new GenericContainer(DockerImageName.parse("hashicorp/vault:1.15"))
      vaultContainer.withEnv("VAULT_DEV_ROOT_TOKEN_ID", "dev-token")
      vaultContainer.withEnv("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
      vaultContainer.withCommand("server", "-dev")
      vaultContainer.withExposedPorts(8200)
      vaultContainer.start()

      val vaultAddr = s"http://${vaultContainer.getHost}:${vaultContainer.getMappedPort(8200)}"
      logger.info(s"Vault container started: $vaultAddr")

      // Set environment variables for tests
      System.setProperty("VAULT_ADDR", vaultAddr)
      System.setProperty("VAULT_TOKEN", "dev-token")
    } catch {
      case ex: Exception =>
        logger.warn(s"Could not start Vault container: ${ex.getMessage}")
        logger.warn("Integration tests requiring Vault will be skipped")
    }

  /**
   * Stops PostgreSQL container.
   */
  private def stopPostgresContainer(): Unit =
    if (postgresContainer != null) {
      try {
        postgresContainer.stop()
        logger.info("PostgreSQL container stopped")
      } catch {
        case ex: Exception =>
          logger.warn(s"Error stopping PostgreSQL container: ${ex.getMessage}")
      }
    }

  /**
   * Stops Vault container.
   */
  private def stopVaultContainer(): Unit =
    if (vaultContainer != null) {
      try {
        vaultContainer.stop()
        logger.info("Vault container stopped")
      } catch {
        case ex: Exception =>
          logger.warn(s"Error stopping Vault container: ${ex.getMessage}")
      }
    }

  /**
   * Cleans up test data between tests.
   */
  private def cleanupTestData(): Unit = {
    // Clean up temp directories
    val tempDirs = Seq(
      "/tmp/test-output",
      "/tmp/test-delta",
      "/tmp/test-checkpoints",
    )

    tempDirs.foreach { dir =>
      try {
        val file = new java.io.File(dir)
        if (file.exists()) {
          deleteRecursively(file)
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Could not clean up directory $dir: ${ex.getMessage}")
      }
    }
  }

  /**
   * Recursively deletes a directory.
   */
  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * Checks if PostgreSQL is available and skips test if not.
   */
  protected def requirePostgres(): Unit =
    assume(
      isPostgresAvailable,
      "PostgreSQL is required for this test but is not running",
    )

  /**
   * Checks if PostgreSQL is available (either docker-compose or Testcontainers).
   */
  protected def isPostgresAvailable: Boolean =
    useDockerCompose || (postgresContainer != null && postgresContainer.isRunning)

  /**
   * Checks if Vault container is available and skips test if not.
   */
  protected def requireVault(): Unit =
    assume(
      isVaultAvailable,
      "Vault is required for this test but is not running",
    )

  /**
   * Checks if Vault is available (either docker-compose or Testcontainers).
   */
  protected def isVaultAvailable: Boolean =
    isServiceRunning(dockerComposeVaultHost, dockerComposeVaultPort) ||
      (vaultContainer != null && vaultContainer.isRunning)

  /**
   * Gets PostgreSQL JDBC URL for tests.
   */
  protected def getPostgresJdbcUrl: String = {
    requirePostgres()
    if (useDockerCompose) {
      s"jdbc:postgresql://$dockerComposePostgresHost:$dockerComposePostgresPort/$dockerComposePostgresDb"
    } else {
      postgresContainer.getJdbcUrl
    }
  }

  /**
   * Gets PostgreSQL connection properties.
   */
  protected def getPostgresProperties: Map[String, Any] = {
    requirePostgres()
    if (useDockerCompose) {
      Map(
        "host"     -> dockerComposePostgresHost,
        "port"     -> dockerComposePostgresPort.toString,
        "database" -> dockerComposePostgresDb,
        "username" -> dockerComposePostgresUser,
        "password" -> dockerComposePostgresPassword,
      )
    } else {
      Map(
        "host"     -> postgresContainer.getHost,
        "port"     -> postgresContainer.getMappedPort(5432).toString,
        "database" -> postgresContainer.getDatabaseName,
        "username" -> postgresContainer.getUsername,
        "password" -> postgresContainer.getPassword,
      )
    }
  }

  /**
   * Gets Vault address for tests.
   */
  protected def getVaultAddress: String = {
    requireVault()
    if (isServiceRunning(dockerComposeVaultHost, dockerComposeVaultPort)) {
      s"http://$dockerComposeVaultHost:$dockerComposeVaultPort"
    } else {
      s"http://${vaultContainer.getHost}:${vaultContainer.getMappedPort(8200)}"
    }
  }

  /**
   * Gets Vault token for tests.
   */
  protected def getVaultToken: String = "dev-token"

  /**
   * Gets PostgreSQL username for connection.
   */
  protected def getPostgresUsername: String =
    if (useDockerCompose) dockerComposePostgresUser else postgresContainer.getUsername

  /**
   * Gets PostgreSQL password for connection.
   */
  protected def getPostgresPassword: String =
    if (useDockerCompose) dockerComposePostgresPassword else postgresContainer.getPassword

  /**
   * Creates a test table in PostgreSQL.
   */
  protected def createTestTable(tableName: String, schema: String): Unit =
    if (isPostgresAvailable) {
      import java.sql.DriverManager

      val connection = DriverManager.getConnection(
        getPostgresJdbcUrl,
        getPostgresUsername,
        getPostgresPassword,
      )

      try {
        val statement = connection.createStatement()
        statement.execute(s"DROP TABLE IF EXISTS $tableName")
        statement.execute(schema)
        logger.info(s"Created test table: $tableName")
      } finally connection.close()
    }

  /**
   * Inserts test data into PostgreSQL.
   */
  protected def insertTestData(tableName: String, data: Seq[Map[String, Any]]): Unit =
    if (isPostgresAvailable && data.nonEmpty) {
      import java.sql.DriverManager

      val connection = DriverManager.getConnection(
        getPostgresJdbcUrl,
        getPostgresUsername,
        getPostgresPassword,
      )

      try {
        val columns      = data.head.keys.toSeq
        val placeholders = columns.map(_ => "?").mkString(", ")
        val sql          = s"INSERT INTO $tableName (${columns.mkString(", ")}) VALUES ($placeholders)"

        val statement = connection.prepareStatement(sql)

        data.foreach { row =>
          columns.zipWithIndex.foreach { case (col, idx) =>
            statement.setObject(idx + 1, row(col))
          }
          statement.addBatch()
        }

        statement.executeBatch()
        logger.info(s"Inserted ${data.size} rows into $tableName")
      } finally connection.close()
    }

  /**
   * Stores a secret in Vault for testing.
   */
  protected def storeVaultSecret(path: String, data: Map[String, Any]): Unit =
    if (isVaultAvailable) {
      import com.pipeline.credentials.VaultClient

      val vaultClient = VaultClient(getVaultAddress, getVaultToken)
      vaultClient.writeSecret(path, data) match {
        case scala.util.Success(_)  =>
          logger.info(s"Stored Vault secret: $path")
        case scala.util.Failure(ex) =>
          logger.error(s"Failed to store Vault secret: $path", ex)
          throw ex
      }
    }

  /**
   * Checks if Docker is available for testing.
   */
  protected def isDockerAvailable: Boolean =
    try {
      val process = Runtime.getRuntime.exec("docker info")
      process.waitFor() == 0
    } catch {
      case _: Exception => false
    }

  /**
   * Skips test if Docker is not available.
   */
  protected def requireDocker(): Unit =
    assume(isDockerAvailable, "Docker is required for this test")
}
