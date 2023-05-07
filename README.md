Fesen HttpClient
[![Java CI with Maven](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fesen-httpclient/actions/workflows/maven.yml)
====================

Fesen HttpClient is HTTP Rest Client for Elasticsearch/OpenSearch.
HttpClient provided by fesen-httpclient is HTTP-based implementation for org.opensearch.client.Client.
This HttpClient supports Elasticsearch 7.x, 8.x and OpenSearch 1.x, 2.x.

Usage
=====

## For Maven

```
<dependency>
    <groupId>org.codelibs.fesen.client</groupId>
    <artifactId>fesen-httpclient</artifactId>
    <version>2.2.0</version>
</dependency>
```

## Example Code

```
// create client
final String host = "http://localhost:9200";
final Settings settings = Settings.builder().putList("http.hosts", host).put("http.compression", true).build();
final HttpClient client = new HttpClient(settings, null);

// search
client.prepareSearch("testindex").setQuery(QueryBuilders.matchAllQuery()).execute(wrap(res -> {
  final long totalHits = res.getHits().getTotalHits().value;
  // ...
}, e -> {
  // ...
}));
```

For the more details, see test code in [src/test/java](https://github.com/codelibs/fesen-httpclient/tree/main/src/test/java/org/codelibs/fesen/client).
