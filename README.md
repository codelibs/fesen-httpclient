# Fesen HttpClient

[![Java CI with Maven](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.codelibs.fesen.client/fesen-httpclient/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.codelibs.fesen.client/fesen-httpclient)
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A high-performance HTTP-based client implementation for OpenSearch and Elasticsearch that provides the standard `org.opensearch.client.Client` interface over HTTP REST APIs. This client enables seamless integration with OpenSearch/Elasticsearch clusters while maintaining full API compatibility.

## 🚀 Key Features

- **Universal Compatibility**: Supports Elasticsearch 7.x, 8.x and OpenSearch 1.x, 2.x, 3.x
- **Standard Interface**: Implements the familiar `org.opensearch.client.Client` interface
- **High Performance**: Built on curl4j library for efficient HTTP communication
- **Production Ready**: Includes connection pooling, failover, health checking, and load balancing
- **Comprehensive Security**: SSL/TLS support, authentication, and proxy configuration
- **Asynchronous Operations**: Full async support with ActionListener callbacks
- **Extensive API Coverage**: Support for all major OpenSearch/Elasticsearch operations

## 📋 Prerequisites

- **Java**: 17 or higher
- **Maven**: 3.6.0 or higher
- **OpenSearch/Elasticsearch**: Compatible with ES 7.x, 8.x and OpenSearch 1.x, 2.x, 3.x

## 🛠 Installation

### Maven Dependency

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>org.codelibs.fesen.client</groupId>
    <artifactId>fesen-httpclient</artifactId>
    <version>3.2.0</version>
</dependency>
```

### Gradle Dependency

Add to your `build.gradle`:

```gradle
implementation 'org.codelibs.fesen.client:fesen-httpclient:3.2.0'
```

## 🏃 Quick Start

### Basic Usage

```java
import org.opensearch.client.Client;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import static org.opensearch.core.action.ActionListener.wrap;

// Create client with basic configuration
Settings settings = Settings.builder()
    .putList("http.hosts", "http://localhost:9200")
    .put("http.compression", true)
    .build();

Client client = new HttpClient(settings, null);

// Perform a search
client.prepareSearch("my_index")
    .setQuery(QueryBuilders.matchAllQuery())
    .execute(wrap(response -> {
        long totalHits = response.getHits().getTotalHits().value;
        System.out.println("Found " + totalHits + " documents");
    }, exception -> {
        System.err.println("Search failed: " + exception.getMessage());
    }));
```

### With Authentication

```java
Settings settings = Settings.builder()
    .putList("http.hosts", "https://opensearch-cluster:9200")
    .put("fesen.username", "admin")
    .put("fesen.password", "admin")
    .put("http.ssl.certificate_authorities", "/path/to/ca.pem")
    .put("http.compression", true)
    .build();

Client client = new HttpClient(settings, null);
```

### Multiple Nodes with Failover

```java
Settings settings = Settings.builder()
    .putList("http.hosts", 
        "http://node1:9200", 
        "http://node2:9200", 
        "http://node3:9200")
    .put("http.heartbeat_interval", 5000L)  // Health check every 5 seconds
    .put("http.compression", true)
    .build();

Client client = new HttpClient(settings, null);
```

## ⚙️ Configuration Options

### Essential Settings

| Setting | Description | Default | Example |
|---------|-------------|---------|---------|
| `http.hosts` | List of OpenSearch/Elasticsearch node URLs | Required | `["http://localhost:9200"]` |
| `http.compression` | Enable GZIP compression | `true` | `true` |
| `http.heartbeat_interval` | Health check interval in milliseconds | `10000` | `5000` |

### Authentication Settings

| Setting | Description | Example |
|---------|-------------|---------|
| `fesen.username` | Basic authentication username | `"admin"` |
| `fesen.password` | Basic authentication password | `"password"` |

### SSL/TLS Settings

| Setting | Description | Example |
|---------|-------------|---------|
| `http.ssl.certificate_authorities` | Path to CA certificate file | `"/path/to/ca.pem"` |

### Proxy Settings

| Setting | Description | Example |
|---------|-------------|---------|
| `http.proxy_host` | Proxy server hostname | `"proxy.example.com"` |
| `http.proxy_port` | Proxy server port | `"8080"` |
| `http.proxy_username` | Proxy authentication username | `"proxyuser"` |
| `http.proxy_password` | Proxy authentication password | `"proxypass"` |

### Thread Pool Settings

| Setting | Description | Default | Example |
|---------|-------------|---------|---------|
| `thread_pool.http.size` | HTTP client thread pool size | CPU cores | `8` |
| `thread_pool.http.async` | Enable async mode for thread pool | `false` | `true` |

## 📚 API Examples

### Document Operations

#### Index a Document
```java
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;

IndexRequest request = new IndexRequest("my_index")
    .id("1")
    .source("{\"name\": \"John Doe\", \"age\": 30}", XContentType.JSON);

client.index(request, wrap(response -> {
    System.out.println("Document indexed: " + response.getId());
}, exception -> {
    System.err.println("Index failed: " + exception.getMessage());
}));
```

#### Get a Document
```java
import org.opensearch.action.get.GetRequest;

GetRequest request = new GetRequest("my_index", "1");

client.get(request, wrap(response -> {
    if (response.isExists()) {
        String source = response.getSourceAsString();
        System.out.println("Document: " + source);
    }
}, exception -> {
    System.err.println("Get failed: " + exception.getMessage());
}));
```

#### Bulk Operations
```java
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;

BulkRequest bulkRequest = new BulkRequest();
for (int i = 0; i < 1000; i++) {
    bulkRequest.add(new IndexRequest("my_index")
        .id(String.valueOf(i))
        .source("{\"id\": " + i + ", \"name\": \"User " + i + "\"}", XContentType.JSON));
}

client.bulk(bulkRequest, wrap(response -> {
    System.out.println("Bulk operation completed with " + response.getItems().length + " items");
}, exception -> {
    System.err.println("Bulk operation failed: " + exception.getMessage());
}));
```

### Search Operations

#### Basic Search
```java
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

SearchRequest request = new SearchRequest("my_index");
SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
sourceBuilder.query(QueryBuilders.termQuery("name.keyword", "John Doe"));
sourceBuilder.size(100);
request.source(sourceBuilder);

client.search(request, wrap(response -> {
    response.getHits().forEach(hit -> {
        System.out.println("Found: " + hit.getSourceAsString());
    });
}, exception -> {
    System.err.println("Search failed: " + exception.getMessage());
}));
```

#### Aggregations
```java
import org.opensearch.search.aggregations.AggregationBuilders;

SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
sourceBuilder.query(QueryBuilders.matchAllQuery());
sourceBuilder.aggregation(
    AggregationBuilders.terms("age_groups").field("age").size(10)
);
sourceBuilder.size(0); // No documents, just aggregations

SearchRequest request = new SearchRequest("my_index");
request.source(sourceBuilder);

client.search(request, wrap(response -> {
    // Process aggregations
    response.getAggregations().forEach(agg -> {
        System.out.println("Aggregation: " + agg.getName());
    });
}, exception -> {
    System.err.println("Aggregation search failed: " + exception.getMessage());
}));
```

#### Scroll API for Large Results
```java
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;

SearchRequest request = new SearchRequest("my_index");
request.scroll(TimeValue.timeValueMinutes(1L));
SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
sourceBuilder.query(QueryBuilders.matchAllQuery());
sourceBuilder.size(1000);
request.source(sourceBuilder);

client.search(request, wrap(response -> {
    String scrollId = response.getScrollId();
    // Process first batch
    processHits(response.getHits());
    
    // Continue scrolling
    continueScrolling(client, scrollId);
}, exception -> {
    System.err.println("Scroll search failed: " + exception.getMessage());
}));

private void continueScrolling(Client client, String scrollId) {
    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
    scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
    
    client.searchScroll(scrollRequest, wrap(response -> {
        if (response.getHits().getHits().length > 0) {
            processHits(response.getHits());
            // Continue with next batch
            continueScrolling(client, response.getScrollId());
        } else {
            // No more results, clear scroll
            client.prepareClearScroll().addScrollId(scrollId).execute();
        }
    }, exception -> {
        System.err.println("Scroll continuation failed: " + exception.getMessage());
    }));
}
```

### Index Management

#### Create Index with Mapping
```java
import org.opensearch.action.admin.indices.create.CreateIndexRequest;

CreateIndexRequest request = new CreateIndexRequest("my_new_index");
request.mapping("""
    {
        "properties": {
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "created_at": {"type": "date"}
        }
    }
    """, XContentType.JSON);

client.admin().indices().create(request, wrap(response -> {
    System.out.println("Index created: " + response.index());
}, exception -> {
    System.err.println("Index creation failed: " + exception.getMessage());
}));
```

#### Update Index Settings
```java
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;

UpdateSettingsRequest request = new UpdateSettingsRequest("my_index");
request.settings(Settings.builder()
    .put("index.refresh_interval", "30s")
    .put("index.number_of_replicas", 2)
);

client.admin().indices().putSettings(request, wrap(response -> {
    System.out.println("Settings updated successfully");
}, exception -> {
    System.err.println("Settings update failed: " + exception.getMessage());
}));
```

### Cluster Operations

#### Cluster Health
```java
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;

ClusterHealthRequest request = new ClusterHealthRequest();
request.indices("my_index");

client.admin().cluster().health(request, wrap(response -> {
    System.out.println("Cluster status: " + response.getStatus());
    System.out.println("Active shards: " + response.getActiveShards());
}, exception -> {
    System.err.println("Health check failed: " + exception.getMessage());
}));
```

## 🏗 Architecture Overview

### Core Components

The Fesen HttpClient is built around several key architectural components:

#### HttpClient
- **Location**: `src/main/java/org/codelibs/fesen/client/HttpClient.java`
- **Purpose**: Main client class that extends `HttpAbstractClient`
- **Responsibilities**: 
  - Implements OpenSearch `Client` interface over HTTP
  - Manages connection pooling, authentication, SSL, and proxy configuration
  - Maps OpenSearch actions to HTTP action implementations

#### NodeManager
- **Location**: `src/main/java/org/codelibs/fesen/client/node/NodeManager.java`
- **Purpose**: Manages multiple OpenSearch node endpoints
- **Features**:
  - Health checking and failover logic
  - Background heartbeat checks to detect node availability
  - Automatic request routing to available nodes

#### HTTP Actions
- **Location**: `src/main/java/org/codelibs/fesen/client/action/`
- **Purpose**: Each OpenSearch API operation has a corresponding `Http*Action` class
- **Function**: Translates OpenSearch requests to HTTP calls and responses back to OpenSearch objects
- **Examples**: `HttpSearchAction`, `HttpIndexAction`, `HttpBulkAction`

### Integration Patterns

- **Action Pattern**: Each OpenSearch operation is implemented as a separate action class
- **Factory Pattern**: `HttpClient` acts as a factory for creating configured HTTP requests
- **Adapter Pattern**: Translates between OpenSearch API and HTTP REST calls
- **Observer Pattern**: Uses ActionListener callbacks for asynchronous operations

## 🧪 Testing

The project includes comprehensive tests for different OpenSearch/Elasticsearch versions:

### Running Tests

```bash
# Run all tests
mvn test

# Run tests for specific OpenSearch version
mvn test -Dtest=OpenSearch3ClientTest

# Run tests with pattern matching
mvn test -Dtest=*ClientTest

# Generate coverage report
mvn jacoco:report
```

### Test Structure

- **Integration Tests**: Version-specific test classes for each supported version
  - `OpenSearch1ClientTest`, `OpenSearch2ClientTest`, `OpenSearch3ClientTest`
  - `Elasticsearch7ClientTest`, `Elasticsearch8ClientTest`
- **TestContainers**: Uses Docker containers to test against real instances
- **Unit Tests**: Component-level tests for utilities and individual actions

### Test Configuration

Tests use TestContainers to spin up real OpenSearch/Elasticsearch instances:

```java
@Test
void testBasicOperations() {
    // TestContainers automatically manages cluster lifecycle
    // Tests run against real OpenSearch instance
}
```

## 🔨 Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/codelibs/fesen-httpclient.git
cd fesen-httpclient

# Compile the project
mvn clean compile

# Run tests
mvn clean test

# Build JAR package
mvn clean package

# Install to local repository
mvn clean install
```

### Code Formatting

The project uses automatic code formatting:

```bash
# Format code using Eclipse formatter
mvn formatter:format

# Apply license headers
mvn license:format
```

### Development Requirements

- **Java Development Kit**: 17 or higher
- **Maven**: 3.6.0 or higher
- **IDE**: Any Java IDE with Maven support
- **Docker**: Required for running integration tests

### Project Structure

```
fesen-httpclient/
├── src/
│   ├── main/java/org/codelibs/fesen/client/
│   │   ├── HttpClient.java              # Main client implementation
│   │   ├── action/                      # HTTP action implementations
│   │   │   ├── HttpSearchAction.java
│   │   │   ├── HttpIndexAction.java
│   │   │   └── ...
│   │   ├── node/                        # Node management
│   │   │   ├── NodeManager.java
│   │   │   └── Node.java
│   │   └── util/                        # Utility classes
│   └── test/java/                       # Test classes
├── pom.xml                              # Maven configuration
├── CLAUDE.md                            # Development instructions
└── README.md                            # This file
```

## 🔧 Advanced Configuration

### Custom Thread Pool Configuration

```java
Settings settings = Settings.builder()
    .putList("http.hosts", "http://localhost:9200")
    .put("thread_pool.http.size", 16)      // Custom thread pool size
    .put("thread_pool.http.async", true)   // Enable async mode
    .build();
```

### SSL Configuration with Custom Trust Store

```java
Settings settings = Settings.builder()
    .putList("http.hosts", "https://secure-cluster:9200")
    .put("http.ssl.certificate_authorities", "/path/to/custom-ca.pem")
    .put("fesen.username", "username")
    .put("fesen.password", "password")
    .build();
```

### Proxy Configuration

```java
Settings settings = Settings.builder()
    .putList("http.hosts", "http://remote-cluster:9200")
    .put("http.proxy_host", "corporate-proxy.company.com")
    .put("http.proxy_port", "8080")
    .put("http.proxy_username", "proxy-user")
    .put("http.proxy_password", "proxy-pass")
    .build();
```

### Custom Request Builders

You can add custom request builders for additional configuration:

```java
HttpClient httpClient = new HttpClient(settings, null);

// Add custom headers to all requests
httpClient.addRequestBuilder(request -> 
    request.header("X-Custom-Header", "MyValue")
);

// Add request logging
httpClient.addRequestBuilder(request -> {
    System.out.println("Making request to: " + request.url());
    return request;
});
```

## ❓ Troubleshooting

### Common Issues

#### Connection Refused
```
org.codelibs.curl.CurlException: Connection refused
```
**Solution**: Verify that OpenSearch/Elasticsearch is running and accessible at the configured host and port.

#### Authentication Failed
```
org.opensearch.OpenSearchStatusException: security_exception
```
**Solution**: Check username/password credentials and ensure the user has necessary permissions.

#### SSL Certificate Issues
```
javax.net.ssl.SSLHandshakeException: sun.security.validator.ValidatorException
```
**Solution**: Verify the certificate authority file path and ensure the certificate is valid.

#### Node Unavailable
```
org.codelibs.fesen.client.node.NodeUnavailableException: No available nodes
```
**Solution**: Check network connectivity and ensure at least one node in the cluster is healthy.

### Enable Debug Logging

Add the following to your logging configuration:

```properties
# For Log4j2
logger.fesen.name = org.codelibs.fesen.client
logger.fesen.level = DEBUG

# For java.util.logging
org.codelibs.fesen.client.level = FINE
```

### Performance Tuning

#### Connection Pool Optimization
```java
Settings settings = Settings.builder()
    .putList("http.hosts", hosts)
    .put("thread_pool.http.size", Runtime.getRuntime().availableProcessors() * 2)
    .put("http.compression", true)
    .put("http.heartbeat_interval", 30000L)  // Reduce health check frequency
    .build();
```

#### Bulk Operation Best Practices
- Use bulk operations for multiple documents
- Batch size of 1000-5000 documents typically works well
- Monitor memory usage with large bulk operations

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow the existing code style and formatting
- Add tests for new features
- Update documentation as needed
- Ensure all tests pass before submitting

## 📞 Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/codelibs/fesen-httpclient/issues)
- **Organization**: [CodeLibs Project](https://www.codelibs.org/)

## 🔗 Related Projects

- **OpenSearch**: [https://opensearch.org/](https://opensearch.org/)
- **Elasticsearch**: [https://www.elastic.co/elasticsearch/](https://www.elastic.co/elasticsearch/)
- **curl4j**: [https://github.com/codelibs/curl4j](https://github.com/codelibs/curl4j)
- **CodeLibs**: [https://www.codelibs.org/](https://www.codelibs.org/)
