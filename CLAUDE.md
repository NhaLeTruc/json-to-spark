# claude-spark-gradle-refined Development Guidelines

**Last Updated:** 2025-11-14

## Project Overview

Production-ready Apache Spark-based ETL/ELT pipeline orchestration framework enabling no-code pipeline creation through JSON configuration files.

## Active Technologies

- **Scala:** 2.12.18 (compatible with Spark 3.5.6)
- **Java:** 17 (with module system workarounds)
- **Apache Spark:** 3.5.6
- **Build Tool:** Gradle 8.5+
- **Security:** HashiCorp Vault integration
- **Testing:** ScalaTest 3.2.17, Testcontainers 1.19.3

## Project Structure

```
src/
├── main/
│   ├── scala/com/pipeline/
│   │   ├── avro/           # Avro conversion utilities
│   │   ├── cli/            # Main entry point (PipelineRunner)
│   │   ├── config/         # JSON configuration parsing
│   │   ├── core/           # Pipeline orchestration (Chain of Responsibility)
│   │   ├── credentials/    # Vault integration & credential management
│   │   ├── exceptions/     # Custom exception hierarchy (8 types)
│   │   ├── metrics/        # Observability & metrics collection
│   │   ├── operations/     # Extract/Load/Transform methods
│   │   ├── retry/          # Retry strategy implementation
│   │   └── security/       # Security policies & auditing
│   └── resources/
│       ├── application.conf # Application configuration (HOCON)
│       └── logback.xml      # Logging configuration
└── test/
    └── scala/com/pipeline/
        ├── contract/        # Schema/contract validation tests
        ├── integration/     # E2E tests with Testcontainers
        ├── performance/     # Performance benchmarking tests
        └── unit/            # Unit tests (130+ tests)
```

## Core Architecture Patterns

### 1. Chain of Responsibility (ADR-001)
Pipeline steps execute sequentially with automatic chaining:
- Each step processes data and passes to next step
- Exception handling at each level
- Automatic retry on transient failures

### 2. Factory Pattern
Credential configuration creation:
- `CredentialConfigFactory` creates typed configs (JDBC, IAM, Other)
- Supports Vault and direct configuration sources

### 3. Immutable Data Structures (ADR-002)
- All configurations are immutable case classes
- Mutable context only for runtime state tracking
- Functional programming style throughout

### 4. Security-First Design (ADR-006)
- Zero credentials in configuration files
- Vault-only enforcement in production (ENVIRONMENT=production)
- Automatic credential sanitization in logs and errors

## Development Workflow

### Building the Project

```bash
# Build standard JAR (includes Spark for local execution)
./gradlew clean build

# Build uber-JAR (excludes Spark for cluster deployment)
./gradlew shadowJar

# Format code (required before commit)
./gradlew scalafmtAll

# Generate documentation
./gradlew scaladoc
```

### Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test categories
./gradlew unitTest           # Unit tests only
./gradlew integrationTest    # Integration tests with Testcontainers
./gradlew performanceTest    # Performance benchmarks
./gradlew contractTest       # Contract validation

# Generate coverage report (85% minimum required)
./gradlew jacocoTestReport
```

### Running Pipelines Locally

```bash
# Using helper script (recommended - includes JVM args)
./run-pipeline.sh config/examples/simple-etl.json

# Manual execution (requires Java 17 module args)
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     [... more args in run-pipeline.sh ...] \
     -jar build/libs/pipeline-app-1.0-SNAPSHOT.jar config.json
```

## Code Style & Conventions

### Scala Style (enforced by scalafmt)

- **Line Length:** 120 characters
- **Indentation:** 2 spaces
- **Imports:** Sorted alphabetically
- **Trailing Commas:** Always use
- **Line Endings:** Unix (LF)

### Naming Conventions

- **Case Classes:** PascalCase (e.g., `PipelineConfig`)
- **Objects:** PascalCase (e.g., `ExtractMethods`)
- **Methods:** camelCase (e.g., `fromPostgres`)
- **Constants:** camelCase (e.g., `maxAttempts`)
- **Type Parameters:** Single uppercase letter (e.g., `T`)

### Error Handling

Always use custom exceptions from `com.pipeline.exceptions`:
- `PipelineException` - Base exception with context
- `CredentialException` - Credential-related errors
- `ValidationException` - Data validation failures
- `DataFrameResolutionException` - Missing DataFrame references
- `RetryableException` - Transient failures
- `ConfigurationException` - Invalid configuration

Example:
```scala
throw new CredentialException(
  "Failed to load credentials",
  credentialPath = Some("secret/data/postgres"),
  credentialType = Some("postgres")
)
```

### Logging Best Practices

```scala
// ✅ GOOD - Use structured logging with context
logger.info(s"Extracting from PostgreSQL: table=$table")

// ✅ GOOD - Add MDC context for tracing
MDC.put("pipelineName", name)
MDC.put("correlationId", uuid)

// ❌ BAD - Never call .count() for logging
logger.info(s"Extracted ${df.count()} rows")  // Triggers full scan!

// ✅ GOOD - Log without expensive operations
logger.info("Successfully extracted data")
```

## Performance Considerations

### Critical Performance Rules

1. **Never call `.count()` in logging** - Triggers full table scan
2. **Use caching strategically** - Configure via `cacheStorageLevel`
3. **Enable partitioning** - Set `partitionColumn` for JDBC sources
4. **Configure fetch sizes** - Use `fetchSize`, `batchSize` for JDBC

### Recommended Configuration

```json
{
  "type": "extract",
  "method": "fromPostgres",
  "config": {
    "table": "large_table",
    "partitionColumn": "id",
    "lowerBound": 0,
    "upperBound": 1000000,
    "numPartitions": 10,
    "fetchSize": 10000,
    "batchSize": 10000,
    "queryTimeout": 300,
    "cache": true,
    "cacheStorageLevel": "MEMORY_AND_DISK"
  }
}
```

### Performance Targets (from spec.md)

- **SC-002:** Simple batch operations ≥100K records/sec
- **SC-003:** Complex batch operations ≥10K records/sec
- **SC-004:** Streaming p95 latency <5 seconds

## Security Guidelines

### Production Environment Detection

Set `ENVIRONMENT=production` to enable strict security:
- Vault credentials required (direct credentials blocked)
- Automatic credential sanitization in all logs
- Security audit trail enabled

### Credential Configuration

```scala
// Production (Vault-only)
{
  "credentialPath": "secret/data/postgres"  // Required
}

// Development (direct credentials allowed)
{
  "host": "localhost",
  "port": 5432,
  "username": "dev_user",
  "password": "dev_pass"  // Only works if ENVIRONMENT != production
}
```

### Vault Setup

```bash
export VAULT_ADDR="http://localhost:8200"
export VAULT_TOKEN="your-vault-token"

# Store credentials
vault kv put secret/postgres \
  host=localhost \
  port=5432 \
  database=mydb \
  username=user \
  password=pass
```

## Testing Strategy

### Test-First Development (TDD)

1. Write failing test first
2. Implement minimum code to pass
3. Refactor while keeping tests green
4. Maintain 85%+ code coverage

### Test Structure

```scala
class MyFeatureTest extends AnyFunSuite with BeforeAndAfterEach {
  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  test("should do something useful") {
    // Arrange
    val input = createTestData()

    // Act
    val result = myFunction(input)

    // Assert
    assert(result.count() == expectedCount)
  }
}
```

### Integration Testing

Uses Testcontainers for realistic testing:
- PostgreSQL container
- MySQL container
- Kafka container
- Vault container

## Common Development Tasks

### Adding a New Extract Method

1. Add method to `ExtractMethods.scala`
2. Update `ExtractStep.extractData()` pattern match
3. Write unit tests in `ExtractMethodsTest.scala`
4. Write integration test if external system involved
5. Update documentation and examples

### Adding a New Transform Method

1. Add method to `UserMethods.scala`
2. Update `TransformStep.transformData()` pattern match
3. Write unit tests in `UserMethodsTest.scala`
4. Add example to `config/examples/`
5. Update README.md

### Adding a New Validation Method

1. Add method to `UserMethods.scala`
2. Update `ValidateStep.validateData()` pattern match
3. Write unit tests
4. Add integration test with validation failure scenarios
5. Document validation rules

## Troubleshooting

### Common Issues

**Build fails with plugin not found:**
- Check internet connectivity
- Verify Gradle version: `gradle --version`
- Try: `gradle wrapper --gradle-version 8.5`

**Tests fail with Java module errors:**
- Tests auto-include required `--add-opens` flags
- Check `build.gradle` test task JVM args

**Spark performance slow:**
- Check for `.count()` calls in logging
- Enable partitioning for large datasets
- Configure caching appropriately

**Vault connection fails:**
- Verify `VAULT_ADDR` and `VAULT_TOKEN` env vars
- Check Vault is running: `vault status`
- Verify network connectivity

## Git Workflow

### Commit Message Format

```
Type: Brief description (50 chars)

Detailed description of changes (wrap at 72 chars).
Explain why this change is needed and what problem it solves.

Impact: Brief impact statement.
```

Types: `Feat`, `Fix`, `Perf`, `Security`, `Docs`, `Test`, `Refactor`, `Chore`

### Before Committing

```bash
# 1. Format code
./gradlew scalafmtAll

# 2. Run tests
./gradlew test

# 3. Check coverage (if applicable)
./gradlew jacocoTestReport

# 4. Stage and commit
git add <files>
git commit -m "Type: Description"
```

## Resources

### Documentation
- **README.md** - User-facing documentation
- **docs/adr/** - Architecture Decision Records (6 ADRs)
- **docs/guides/** - User guides and tutorials
- **docs/features/** - Feature documentation
- **specs/** - Detailed specifications

### Example Pipelines
- `config/examples/simple-etl.json` - Basic ETL
- `config/examples/multi-source-join.json` - Multi-source join
- `config/examples/streaming-kafka.json` - Streaming pipeline
- `config/examples/data-quality-pipeline.json` - Validation examples

### Key Files to Understand
1. **Pipeline.scala** - Main orchestrator
2. **PipelineStep.scala** - Step execution chain
3. **ExtractMethods.scala** - Data source extraction
4. **LoadMethods.scala** - Data sink loading
5. **UserMethods.scala** - Transformations & validations

## Recent Changes

**2025-11-14 - Codebase Review Improvements**
- ✅ Removed expensive `.count()` operations
- ✅ Standardized test configuration
- ✅ Added environment-based security validation
- ✅ Added JDBC connection pooling
- ✅ Added DataFrame validation after extraction
- ✅ Fixed storage level parsing error handling

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->

---

**Questions?** See README.md troubleshooting or project documentation in docs/.
