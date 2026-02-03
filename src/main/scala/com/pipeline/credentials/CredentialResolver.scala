package com.pipeline.credentials

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
 * Shared utility for resolving credentials from Vault or config.
 *
 * Consolidates duplicated credential resolution logic from ExtractMethods and LoadMethods.
 * Uses a lazy singleton VaultClient to avoid creating new instances on every call.
 */
object CredentialResolver {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Lazy singleton VaultClient - only initialized when Vault credentials are actually needed
  private lazy val defaultVaultClient: VaultClient = VaultClient.fromEnv()

  private def getVaultClient(vaultClient: Option[VaultClient]): VaultClient =
    vaultClient.getOrElse(defaultVaultClient)

  /**
   * Resolves JDBC credentials from Vault or config.
   *
   * @param config         Configuration map
   * @param credentialType JDBC type ("postgres" or "mysql")
   * @param vaultClient    Optional VaultClient override (for testing)
   * @return JdbcConfig instance
   */
  def resolveJdbcCredentials(
      config: Map[String, Any],
      credentialType: String,
      vaultClient: Option[VaultClient] = None,
  ): JdbcConfig =
    config.get("credentialPath") match {
      case Some(path) =>
        logger.info(s"Resolving JDBC credentials from Vault: $path")
        getVaultClient(vaultClient).readSecret(path.toString) match {
          case Success(data) =>
            CredentialConfigFactory.create(credentialType, data).asInstanceOf[JdbcConfig]
          case Failure(ex) =>
            throw new RuntimeException(s"Failed to read credentials from Vault: ${ex.getMessage}", ex)
        }
      case None =>
        logger.warn("Using credentials from config - not recommended for production")
        JdbcConfig(
          host = config.getOrElse("host", "localhost").toString,
          port = config.getOrElse("port", if (credentialType == "postgres") 5432 else 3306).toString.toInt,
          database = config.getOrElse("database", "").toString,
          username = config.getOrElse("username", "").toString,
          password = config.getOrElse("password", "").toString,
          credentialType = credentialType,
          properties = config.getOrElse("properties", Map.empty[String, String]).asInstanceOf[Map[String, String]],
        )
    }

  /**
   * Resolves S3/IAM credentials from Vault or config.
   *
   * @param config      Configuration map
   * @param vaultClient Optional VaultClient override (for testing)
   * @return IAMConfig instance
   */
  def resolveS3Credentials(
      config: Map[String, Any],
      vaultClient: Option[VaultClient] = None,
  ): IAMConfig =
    config.get("credentialPath") match {
      case Some(path) =>
        logger.info(s"Resolving S3 credentials from Vault: $path")
        getVaultClient(vaultClient).readSecret(path.toString) match {
          case Success(data) =>
            CredentialConfigFactory.create("s3", data).asInstanceOf[IAMConfig]
          case Failure(ex) =>
            throw new RuntimeException(s"Failed to read S3 credentials from Vault: ${ex.getMessage}", ex)
        }
      case None =>
        logger.warn("Using S3 credentials from config - not recommended for production")
        IAMConfig(
          accessKeyId = config.getOrElse("accessKeyId", "").toString,
          secretAccessKey = config.getOrElse("secretAccessKey", "").toString,
          sessionToken = config.get("sessionToken").map(_.toString),
          region = config.getOrElse("region", "us-east-1").toString,
        )
    }

  /**
   * Resolves Kafka credentials from Vault or config.
   *
   * @param config      Configuration map
   * @param vaultClient Optional VaultClient override (for testing)
   * @return Map of Kafka configuration properties
   */
  def resolveKafkaCredentials(
      config: Map[String, Any],
      vaultClient: Option[VaultClient] = None,
  ): Map[String, Any] =
    config.get("credentialPath") match {
      case Some(path) =>
        logger.info(s"Resolving Kafka credentials from Vault: $path")
        getVaultClient(vaultClient).readSecret(path.toString) match {
          case Success(data) => data
          case Failure(ex) =>
            throw new RuntimeException(s"Failed to read Kafka credentials from Vault: ${ex.getMessage}", ex)
        }
      case None =>
        config
    }
}