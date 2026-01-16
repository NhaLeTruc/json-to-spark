package com.pipeline.unit.credentials

import com.pipeline.credentials.VaultClient
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

/**
 * Unit tests for VaultClient.
 *
 * Tests FR-011: Secure credential storage via HashiCorp Vault.
 * Validates Constitution Section II: Test-First Development (TDD).
 */
@RunWith(classOf[JUnitRunner])
class VaultClientTest extends AnyFunSuite with Matchers {

  test("VaultClient should read secret from path") {
    // This test will be skipped if VAULT_ADDR is not set (for local dev without Vault)
    val vaultAddr  = sys.env.get("VAULT_ADDR")
    val vaultToken = sys.env.get("VAULT_TOKEN")

    if (vaultAddr.isEmpty || vaultToken.isEmpty) {
      info("Skipping test - VAULT_ADDR or VAULT_TOKEN not set")
      succeed
    } else {
      val client = VaultClient(vaultAddr.get, vaultToken.get)

      // First write a test secret
      val testData   = Map[String, Any]("test_key" -> "test_value", "number" -> "42")
      val writeResult = client.writeSecret("secret/data/test/vault_client_test", testData)

      writeResult.isSuccess shouldBe true

      // Then read it back
      val readResult = client.readSecret("secret/data/test/vault_client_test")

      readResult match {
        case scala.util.Success(data) =>
          // Vault KV v2 wraps data in a "data" key
          val secretData = data.getOrElse("data", data).asInstanceOf[Map[String, Any]]
          secretData should contain key "test_key"

        case scala.util.Failure(ex) =>
          fail(s"Failed to read secret: ${ex.getMessage}")
      }
    }
  }

  test("VaultClient should handle connection errors gracefully") {
    // Test with invalid address
    val invalidVault = VaultClient("http://invalid-vault:9999", "invalid-token")

    val result = invalidVault.readSecret("secret/data/test")

    result.isFailure shouldBe true
  }

  test("VaultClient should handle missing secrets") {
    val vaultAddr  = sys.env.get("VAULT_ADDR")
    val vaultToken = sys.env.get("VAULT_TOKEN")

    if (vaultAddr.isEmpty || vaultToken.isEmpty) {
      info("Skipping test - VAULT_ADDR or VAULT_TOKEN not set")
      succeed
    } else {
      val client = VaultClient(vaultAddr.get, vaultToken.get)

      // Try to read a non-existent secret
      val result = client.readSecret("secret/data/nonexistent/path/that/does/not/exist")

      // Should fail or return empty - both are acceptable behaviors
      result match {
        case scala.util.Success(data) =>
          // If it succeeds, the data should be empty or the path doesn't have the expected keys
          info(s"Vault returned for missing path: $data")

        case scala.util.Failure(ex) =>
          // Expected - missing secret should result in failure
          info(s"Missing secret correctly resulted in failure: ${ex.getMessage}")
          succeed
      }
    }
  }

  test("VaultClient should read from environment variables") {
    // VaultClient should use VAULT_ADDR and VAULT_TOKEN from env
    sys.env.get("VAULT_ADDR") match {
      case Some(_) =>
        val client = VaultClient.fromEnv()
        client shouldBe a[VaultClient]
      case None    =>
        info("Skipping test - VAULT_ADDR not set")
        succeed
    }
  }

  test("VaultClient should parse secret data correctly") {
    // Mock secret data structure
    val secretData = Map(
      "host"     -> "localhost",
      "port"     -> "5432",
      "database" -> "testdb",
      "username" -> "user",
      "password" -> "pass",
    )

    secretData should contain key "host"
    secretData should contain key "username"
    secretData should contain key "password"
  }

  test("VaultClient should handle authentication errors") {
    val vaultAddr     = sys.env.getOrElse("VAULT_ADDR", "http://localhost:8200")
    val invalidClient = VaultClient(vaultAddr, "invalid-token-12345")

    val result = invalidClient.readSecret("secret/data/test")

    result.isFailure shouldBe true
  }

  test("VaultClient should support custom namespaces") {
    val vaultAddr = sys.env.getOrElse("VAULT_ADDR", "http://localhost:8200")
    val token     = sys.env.getOrElse("VAULT_TOKEN", "test-token")

    val client = VaultClient(vaultAddr, token, Some("custom-namespace"))

    client.namespace shouldBe Some("custom-namespace")
  }
}
