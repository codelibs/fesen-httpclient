# Fesen HttpClient

[![Java CI with Maven](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.codelibs.fesen.client/fesen-httpclient/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.codelibs.fesen.client/fesen-httpclient)
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Fesen HttpClient is a Java implementation of the `org.opensearch.client.Client` interface that talks to OpenSearch or Elasticsearch over their HTTP REST APIs instead of the native transport protocol. It lets applications written against the standard OpenSearch client API run against any HTTP-reachable cluster, including managed services that only expose REST endpoints.

The library is built on [curl4j](https://github.com/codelibs/curl4j), a lightweight, pure-Java HTTP client, so it has no dependency on the OpenSearch/Elasticsearch transport module.

## Features

- Implements `org.opensearch.client.Client`, so existing code written against that interface can switch transports with minimal changes.
- Works against Elasticsearch 7.x and 8.x, and OpenSearch 1.x, 2.x, and 3.x. See [Supported Engines](#supported-engines) for the exact versions covered by the test suite.
- Detects the backend engine and version automatically from the cluster's root endpoint response.
- Supports multiple nodes with health checking and failover.
- Supports HTTP Basic authentication, TLS, and HTTP proxies.
- Covers most administrative and data APIs: documents, search, bulk, scroll, point-in-time, index and template management, cluster and node APIs, snapshots, ingest pipelines, and more.

## Requirements

- Java 17 or higher
- Maven 3.6.0 or higher (for building from source)
- Docker (only needed to run the integration test suite)

## Installation

### Maven

```xml
<dependency>
    <groupId>org.codelibs.fesen.client</groupId>
    <artifactId>fesen-httpclient</artifactId>
    <version>3.7.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'org.codelibs.fesen.client:fesen-httpclient:3.7.0'
```

Check the Maven Central badge above for the latest published version.

## Quick Start

```java
import org.opensearch.client.Client;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import static org.opensearch.core.action.ActionListener.wrap;

Settings settings = Settings.builder()
    .putList("http.hosts", "http://localhost:9200")
    .put("http.compression", true)
    .build();

Client client = new HttpClient(settings, null);

client.prepareSearch("my_index")
    .setQuery(QueryBuilders.matchAllQuery())
    .execute(wrap(response -> {
        long totalHits = response.getHits().getTotalHits().value();
        System.out.println("Found " + totalHits + " documents");
    }, exception -> {
        System.err.println("Search failed: " + exception.getMessage());
    }));

// client.close() when the client is no longer needed
```

The second constructor argument is a `ThreadPool`. Passing `null` is fine; `HttpClient` creates its own internal thread pool from the `thread_pool.http.*` settings.

## Configuration

`http.hosts` is the only required setting. Everything else has a sensible default.

### Connection

| Setting | Description | Default |
|---|---|---|
| `http.hosts` | List of node URLs (`http://` is assumed if the scheme is omitted) | required |
| `http.compression` | Enable GZIP compression | `true` |
| `http.heartbeat_interval` | Node health-check interval, in milliseconds | `10000` |
| `http.connection_timeout` | Connection timeout, in milliseconds (`0` = no timeout) | `0` |
| `http.socket_timeout` | Socket read timeout, in milliseconds (`0` = no timeout) | `0` |

### Authentication and TLS

| Setting | Description |
|---|---|
| `fesen.username` | HTTP Basic authentication username |
| `fesen.password` | HTTP Basic authentication password |
| `http.ssl.certificate_authorities` | Path to a CA certificate file used to validate the server's TLS certificate |

### Proxy

| Setting | Description |
|---|---|
| `http.proxy_host` | Proxy server hostname |
| `http.proxy_port` | Proxy server port |
| `http.proxy_username` | Proxy authentication username |
| `http.proxy_password` | Proxy authentication password |

### Thread pool

| Setting | Description | Default |
|---|---|---|
| `thread_pool.http.size` | Size of the internal HTTP thread pool | number of CPU cores |
| `thread_pool.http.async` | Run requests asynchronously through the thread pool | `false` |

### Example: multiple nodes with authentication and TLS

```java
Settings settings = Settings.builder()
    .putList("http.hosts",
        "https://node1:9200",
        "https://node2:9200",
        "https://node3:9200")
    .put("http.heartbeat_interval", 5000L)
    .put("fesen.username", "admin")
    .put("fesen.password", "admin")
    .put("http.ssl.certificate_authorities", "/path/to/ca.pem")
    .build();

Client client = new HttpClient(settings, null);
```

### Custom request headers

`HttpClient.addRequestBuilder` registers a function that is applied to every outgoing curl4j request, which is useful for adding headers such as API keys:

```java
import org.codelibs.curl.CurlRequest;

HttpClient httpClient = new HttpClient(settings, null);
httpClient.addRequestBuilder(request -> request.header("X-Custom-Header", "MyValue"));
```

## Usage Examples

### Index a document

```java
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;

IndexRequest request = new IndexRequest("my_index")
    .id("1")
    .source("{\"name\": \"John Doe\", \"age\": 30}", XContentType.JSON);

client.index(request, wrap(
    response -> System.out.println("Indexed: " + response.getId()),
    exception -> System.err.println("Index failed: " + exception.getMessage())));
```

### Get a document

```java
import org.opensearch.action.get.GetRequest;

GetRequest request = new GetRequest("my_index", "1");

client.get(request, wrap(response -> {
    if (response.isExists()) {
        System.out.println("Document: " + response.getSourceAsString());
    }
}, exception -> System.err.println("Get failed: " + exception.getMessage())));
```

### Bulk indexing

```java
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;

BulkRequest bulkRequest = new BulkRequest();
for (int i = 0; i < 1000; i++) {
    bulkRequest.add(new IndexRequest("my_index")
        .id(String.valueOf(i))
        .source("{\"id\": " + i + "}", XContentType.JSON));
}

client.bulk(bulkRequest, wrap(
    response -> System.out.println("Indexed " + response.getItems().length + " documents"),
    exception -> System.err.println("Bulk request failed: " + exception.getMessage())));
```

### Create an index

```java
import org.opensearch.action.admin.indices.create.CreateIndexRequest;

CreateIndexRequest request = new CreateIndexRequest("my_new_index");
request.mapping("""
    {
        "properties": {
            "name": {"type": "text"},
            "created_at": {"type": "date"}
        }
    }
    """, XContentType.JSON);

client.admin().indices().create(request, wrap(
    response -> System.out.println("Created: " + response.index()),
    exception -> System.err.println("Create failed: " + exception.getMessage())));
```

### Cluster health

```java
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;

client.admin().cluster().health(new ClusterHealthRequest(), wrap(
    response -> System.out.println("Status: " + response.getStatus()),
    exception -> System.err.println("Health check failed: " + exception.getMessage())));
```

Search, scroll, point-in-time, aggregations, and other read/write operations follow the same pattern using the corresponding request classes from `org.opensearch.action.*`. `HttpClient` covers close to the full REST API surface; see the `action` package described below for the complete list.

## Supported Engines

The integration test suite runs against the following versions using Testcontainers:

| Engine | Versions tested |
|---|---|
| OpenSearch | 1.3.20, 2.19.4, 3.7.0 |
| Elasticsearch | 7.17.29, 8.19.11 |

`HttpClient` inspects the cluster's root endpoint (`GET /`) to determine which engine and major version it is talking to, and adjusts request/response handling where the two diverge.

## How It Works

- `HttpClient` (`src/main/java/org/codelibs/fesen/client/HttpClient.java`) is the entry point. It implements the OpenSearch `Client` interface, owns the connection settings (authentication, TLS, proxy, thread pool), and dispatches each action type to its HTTP implementation.
- `NodeManager` (`.../client/node/`) tracks the configured nodes, runs periodic heartbeat checks, and routes requests away from nodes that are currently unavailable.
- `action` (`.../client/action/`) contains one `Http*Action` class per REST API operation (for example `HttpSearchAction`, `HttpIndexAction`, `HttpBulkAction`). Each class builds the HTTP request for its operation and parses the response back into the matching OpenSearch response object.
- `EngineInfo` and `HttpAdminClient`/`HttpIndicesAdminClient` handle engine detection and the cluster/indices admin APIs respectively.
- `curl` (`.../client/curl/`) wraps [curl4j](https://github.com/codelibs/curl4j) requests with the settings configured on `HttpClient`.

## Building and Testing

```bash
git clone https://github.com/codelibs/fesen-httpclient.git
cd fesen-httpclient

mvn clean compile      # compile
mvn clean test          # run the test suite (requires Docker; see below)
mvn clean package       # build the jar
mvn clean install       # install to the local repository
```

The test suite uses [Testcontainers](https://testcontainers.com/) to start real OpenSearch/Elasticsearch instances in Docker, one version-specific test class per engine (`OpenSearch1ClientTest`, `OpenSearch2ClientTest`, `OpenSearch3ClientTest`, `Elasticsearch7ClientTest`, `Elasticsearch8ClientTest`). Docker must be running locally. To run a single engine's tests:

```bash
mvn test -Dtest=OpenSearch3ClientTest
```

Before committing, format the code and apply license headers:

```bash
mvn formatter:format
mvn license:format
```

A coverage report is produced automatically under `target/site/jacoco` during `mvn package`, or on demand with `mvn jacoco:report`.

## Troubleshooting

**Connection refused** (`org.codelibs.curl.CurlException: Connection refused`)
Verify that the cluster is running and reachable at the host and port configured in `http.hosts`.

**Authentication failed** (`org.opensearch.OpenSearchStatusException: security_exception`)
Check the `fesen.username`/`fesen.password` credentials and the permissions of that user.

**TLS handshake failure** (`javax.net.ssl.SSLHandshakeException`)
Check the `http.ssl.certificate_authorities` path and make sure the certificate is valid and trusted.

**No available nodes** (`org.codelibs.fesen.client.node.NodeUnavailableException`)
All configured nodes are currently failing health checks. Verify network connectivity and cluster health, and consider adjusting `http.heartbeat_interval`.

**Debug logging** — for Log4j2:

```properties
logger.fesen.name = org.codelibs.fesen.client
logger.fesen.level = DEBUG
```

## Contributing

Contributions are welcome. For anything beyond a small fix, please open an issue first to discuss the change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes, with tests
4. Run `mvn formatter:format && mvn license:format` and `mvn clean test`
5. Open a pull request

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Links

- [Issue tracker](https://github.com/codelibs/fesen-httpclient/issues)
- [CodeLibs Project](https://www.codelibs.org/)
- [OpenSearch](https://opensearch.org/)
- [Elasticsearch](https://www.elastic.co/elasticsearch/)
- [curl4j](https://github.com/codelibs/curl4j)
