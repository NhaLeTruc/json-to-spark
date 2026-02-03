package com.pipeline.security

import org.slf4j.{Logger, LoggerFactory, MDC}

import java.time.Instant

/**
 * Audit log entry for credential access.
 *
 * Tracks who accessed what credentials and when.
 */
case class CredentialAuditEntry(
    timestamp: Long = System.currentTimeMillis(),
    pipelineName: Option[String],
    credentialPath: String,
    credentialType: String,
    accessType: String, // "read", "write", "delete"
    source: String,     // "vault", "plaintext", "environment"
    success: Boolean,
    errorMessage: Option[String] = None,
    userId: Option[String] = None,
    ipAddress: Option[String] = None,
) {

  /**
   * Converts audit entry to map for logging.
   */
  def toMap: Map[String, Any] =
    Map(
      "timestamp"      -> timestamp,
      "timestampIso"   -> Instant.ofEpochMilli(timestamp).toString,
      "pipelineName"   -> pipelineName.getOrElse("unknown"),
      "credentialPath" -> credentialPath,
      "credentialType" -> credentialType,
      "accessType"     -> accessType,
      "source"         -> source,
      "success"        -> success,
      "errorMessage"   -> errorMessage.getOrElse(""),
      "userId"         -> userId.getOrElse("unknown"),
      "ipAddress"      -> ipAddress.getOrElse("unknown"),
    )

  /**
   * Converts audit entry to JSON string.
   */
  def toJson: String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    write(toMap)
  }
}

/**
 * Audit logger for credential access.
 *
 * Implements Sprint 3-4 Task 2.3.2: Credential Access Auditing.
 */
object CredentialAudit {

  private val logger: Logger = LoggerFactory.getLogger("CredentialAudit")

  // In-memory audit log (thread-safe)
  private val auditLog = new java.util.concurrent.ConcurrentLinkedQueue[CredentialAuditEntry]()

  /**
   * Logs a credential access event.
   *
   * @param entry Audit entry to log
   */
  def log(entry: CredentialAuditEntry): Unit = {
    // Add to in-memory log
    auditLog.add(entry)

    // Log via SLF4J with MDC context
    try {
      MDC.put("auditType", "credential_access")
      MDC.put("credentialPath", entry.credentialPath)
      MDC.put("credentialType", entry.credentialType)
      MDC.put("accessType", entry.accessType)
      MDC.put("source", entry.source)
      MDC.put("success", entry.success.toString)
      entry.pipelineName.foreach(name => MDC.put("pipelineName", name))
      entry.userId.foreach(user => MDC.put("userId", user))

      if (entry.success) {
        logger.info(
          s"Credential access: path=${entry.credentialPath}, type=${entry.credentialType}, " +
            s"access=${entry.accessType}, source=${entry.source}",
        )
      } else {
        logger.warn(
          s"Credential access FAILED: path=${entry.credentialPath}, type=${entry.credentialType}, " +
            s"error=${entry.errorMessage.getOrElse("unknown")}",
        )
      }
    } finally MDC.clear()
  }

  /**
   * Logs successful credential read from Vault.
   */
  def logVaultRead(
      path: String,
      credentialType: String,
      pipelineName: Option[String] = None,
  ): Unit =
    log(
      CredentialAuditEntry(
        pipelineName = pipelineName,
        credentialPath = path,
        credentialType = credentialType,
        accessType = "read",
        source = "vault",
        success = true,
      ),
    )

  /**
   * Logs failed credential read from Vault.
   */
  def logVaultReadFailure(
      path: String,
      credentialType: String,
      error: String,
      pipelineName: Option[String] = None,
  ): Unit =
    log(
      CredentialAuditEntry(
        pipelineName = pipelineName,
        credentialPath = path,
        credentialType = credentialType,
        accessType = "read",
        source = "vault",
        success = false,
        errorMessage = Some(error),
      ),
    )

  /**
   * Logs plain text credential usage.
   */
  def logPlainTextAccess(
      credentialType: String,
      pipelineName: Option[String] = None,
  ): Unit =
    log(
      CredentialAuditEntry(
        pipelineName = pipelineName,
        credentialPath = "inline",
        credentialType = credentialType,
        accessType = "read",
        source = "plaintext",
        success = true,
      ),
    )

  /**
   * Logs environment variable credential usage.
   */
  def logEnvironmentAccess(
      envVar: String,
      credentialType: String,
      pipelineName: Option[String] = None,
  ): Unit =
    log(
      CredentialAuditEntry(
        pipelineName = pipelineName,
        credentialPath = envVar,
        credentialType = credentialType,
        accessType = "read",
        source = "environment",
        success = true,
      ),
    )

  /**
   * Logs security policy violation.
   */
  def logPolicyViolation(
      violation: String,
      credentialPath: String,
      credentialType: String,
      pipelineName: Option[String] = None,
  ): Unit =
    log(
      CredentialAuditEntry(
        pipelineName = pipelineName,
        credentialPath = credentialPath,
        credentialType = credentialType,
        accessType = "read",
        source = "policy_violation",
        success = false,
        errorMessage = Some(violation),
      ),
    )

  /**
   * Gets all audit entries (for testing).
   *
   * @return List of audit entries
   */
  def getAuditLog: List[CredentialAuditEntry] = {
    import scala.jdk.CollectionConverters._
    auditLog.asScala.toList
  }

  /**
   * Clears the audit log (for testing).
   */
  def clearAuditLog(): Unit =
    auditLog.clear()

  /**
   * Exports audit log to JSON file.
   *
   * @param outputPath Path to write JSON file
   */
  def exportToJson(outputPath: String): Unit = {
    val entries = getAuditLog
    val json    = entries.map(_.toJson).mkString("[\n  ", ",\n  ", "\n]")

    val writer = new java.io.PrintWriter(new java.io.File(outputPath))
    try {
      writer.write(json)
      logger.info(s"Exported ${entries.size} audit entries to: $outputPath")
    } finally writer.close()
  }

  /**
   * Exports audit log to JSON Lines file.
   *
   * @param outputPath Path to write JSONL file
   * @param append     Append to existing file
   */
  def exportToJsonLines(outputPath: String, append: Boolean = true): Unit = {
    import java.nio.file.{Files, Paths, StandardOpenOption}

    val entries = getAuditLog
    val lines   = entries.map(_.toJson).mkString("", "\n", "\n")

    val path    = Paths.get(outputPath)
    val options = if (append) {
      Seq(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    } else {
      Seq(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }

    Files.write(path, lines.getBytes("UTF-8"), options: _*)
    logger.info(s"Exported ${entries.size} audit entries to: $outputPath (append=$append)")
  }
}
