package com.pipeline.credentials

import com.bettercloud.vault.{Vault, VaultConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Client for HashiCorp Vault operations.
 *
 * Implements FR-011: Secure credential storage via HashiCorp Vault.
 * Validates Constitution Section VII: Security and credential management.
 *
 * @param address   Vault server address
 * @param token     Vault authentication token
 * @param namespace Optional Vault namespace
 */
case class VaultClient(
    address: String,
    token: String,
    namespace: Option[String] = None,
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val config: VaultConfig = {
    val builder = new VaultConfig()
      .address(address)
      .token(token)

    namespace.foreach(builder.nameSpace)

    builder.build()
  }

  private val vault: Vault = new Vault(config)

  /**
   * Reads a secret from Vault at the given path.
   *
   * Path should follow Vault's KV v2 format: "secret/data/..."
   *
   * @param path Vault secret path
   * @return Try containing Map of secret data or failure
   */
  def readSecret(path: String): Try[Map[String, Any]] =
    Try {
      logger.info(s"Reading secret from Vault: $path")

      val response = vault.logical().read(path)

      if (response.getRestResponse.getStatus != 200) {
        throw new RuntimeException(
          s"Failed to read secret from Vault: ${response.getRestResponse.getStatus}",
        )
      }

      val data: Map[String, String] = response.getData.asScala.toMap

      logger.info(s"Successfully retrieved secret from Vault: $path")

      data.map { case (k, v) => k -> (v: Any) }
    }.recoverWith { case ex: Exception =>
      logger.error(s"Error reading secret from Vault at path $path: ${ex.getMessage}", ex)
      Failure(ex)
    }

  /**
   * Writes a secret to Vault at the given path.
   *
   * @param path Vault secret path
   * @param data Secret data to write
   * @return Try indicating success or failure
   */
  def writeSecret(path: String, data: Map[String, Any]): Try[Unit] =
    Try {
      logger.info(s"Writing secret to Vault: $path")

      val javaData = data.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.asJava

      val response = vault.logical().write(path, javaData)

      if (response.getRestResponse.getStatus != 200 && response.getRestResponse.getStatus != 204) {
        throw new RuntimeException(
          s"Failed to write secret to Vault: ${response.getRestResponse.getStatus}",
        )
      }

      logger.info(s"Successfully wrote secret to Vault: $path")
    }.recoverWith { case ex: Exception =>
      logger.error(s"Error writing secret to Vault at path $path: ${ex.getMessage}", ex)
      Failure(ex)
    }
}

/**
 * Factory methods for VaultClient creation.
 */
object VaultClient {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates VaultClient from environment variables.
   *
   * Reads VAULT_ADDR, VAULT_TOKEN, and optionally VAULT_NAMESPACE.
   *
   * @return VaultClient instance
   * @throws IllegalStateException if required environment variables are missing
   */
  def fromEnv(): VaultClient = {
    val address = sys.env.getOrElse(
      "VAULT_ADDR",
      throw new IllegalStateException("VAULT_ADDR environment variable is not set"),
    )

    val token = sys.env.getOrElse(
      "VAULT_TOKEN",
      throw new IllegalStateException("VAULT_TOKEN environment variable is not set"),
    )

    val namespace = sys.env.get("VAULT_NAMESPACE")

    logger.info(s"Creating VaultClient from environment: address=$address, namespace=$namespace")

    VaultClient(address, token, namespace)
  }

  /**
   * Creates VaultClient with optional configuration.
   *
   * Falls back to environment variables if not provided.
   *
   * @param address   Optional Vault address
   * @param token     Optional Vault token
   * @param namespace Optional Vault namespace
   * @return VaultClient instance
   */
  def fromConfig(
      address: Option[String] = None,
      token: Option[String] = None,
      namespace: Option[String] = None,
  ): VaultClient = {
    val finalAddress = address.orElse(sys.env.get("VAULT_ADDR")).getOrElse("http://localhost:8200")
    val finalToken = token
      .orElse(sys.env.get("VAULT_TOKEN"))
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalStateException(
        "Vault token not provided and VAULT_TOKEN environment variable is not set"
      ))
    val finalNamespace = namespace.orElse(sys.env.get("VAULT_NAMESPACE"))

    VaultClient(finalAddress, finalToken, finalNamespace)
  }
}
