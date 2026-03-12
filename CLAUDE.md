# fesen-httpclient

HTTP client library for OpenSearch/Elasticsearch. Provides a unified Java API
that works with OpenSearch 1.x/2.x/3.x and Elasticsearch 7.x/8.x over HTTP.

## Build & Test

```bash
mvn install                    # Build and run tests
mvn install -DskipTests        # Build without tests
mvn test                       # Run tests only (requires Docker)
```

Tests use Testcontainers and require Docker to be running.

## Project Structure

- `src/main/java/.../client/` - Core client classes
  - `HttpClient.java` - Main client implementation
  - `HttpAbstractClient.java` - Base class with shared logic
  - `HttpAdminClient.java` / `HttpIndicesAdminClient.java` - Admin APIs
  - `EngineInfo.java` - Backend engine detection (OpenSearch vs Elasticsearch)
  - `action/` - HTTP action implementations for each API endpoint
  - `curl/` - Low-level HTTP request handling
  - `node/` - Node discovery and management
- `src/test/java/` - Integration tests per backend version

## Key Details

- Java 17+ required (compiler target)
- Supports multiple backends: OpenSearch 1/2/3, Elasticsearch 7/8
- Uses maven-release-plugin for versioning
- Run `mvn formatter:format && mvn license:format` before committing
