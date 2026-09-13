/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.codelibs.fesen.opensearch.core.action.ActionListener.wrap;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.client.action.HttpNodesStatsAction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.codelibs.fesen.opensearch.action.DocWriteResponse.Result;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.codelibs.fesen.opensearch.action.bulk.BulkRequestBuilder;
import org.codelibs.fesen.opensearch.action.bulk.BulkResponse;
import org.codelibs.fesen.opensearch.action.delete.DeleteResponse;
import org.codelibs.fesen.opensearch.action.explain.ExplainResponse;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilities;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.codelibs.fesen.opensearch.action.get.GetResponse;
import org.codelibs.fesen.opensearch.action.get.MultiGetRequest;
import org.codelibs.fesen.opensearch.action.get.MultiGetRequestBuilder;
import org.codelibs.fesen.opensearch.action.get.MultiGetResponse;
import org.codelibs.fesen.opensearch.action.index.IndexResponse;
import org.codelibs.fesen.opensearch.action.search.ClearScrollResponse;
import org.codelibs.fesen.opensearch.action.search.MultiSearchResponse;
import org.codelibs.fesen.opensearch.action.search.SearchRequestBuilder;
import org.codelibs.fesen.opensearch.action.search.SearchResponse;
import org.codelibs.fesen.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.update.UpdateResponse;
import org.codelibs.fesen.opensearch.cluster.metadata.MappingMetadata;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeUnit;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.IndexNotFoundException;
import org.codelibs.fesen.opensearch.index.query.QueryBuilders;
import org.codelibs.fesen.opensearch.search.SearchHit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Deprecated
class Elasticsearch7ClientTest {
    static final Logger logger = Logger.getLogger(Elasticsearch7ClientTest.class.getName());

    static final String version = "7.17.29";

    static final String imageTag = "docker.elastic.co/elasticsearch/elasticsearch:" + version;

    static String clusterName = "docker-cluster";

    static GenericContainer server;

    private HttpClient client;

    @BeforeAll
    static void setUpAll() {
        setupLogger();
        startServer();
        waitFor();
    }

    static void setupLogger() {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
        final Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.INFO);
        final ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.INFO);
        rootLogger.addHandler(handler);
    }

    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse(imageTag))//
                .withEnv("discovery.type", "single-node")//
                .withExposedPorts(9200)//
                .withStartupAttempts(3);
        server.start();
    }

    static void waitFor() {
        final String url = "http://" + server.getHost() + ":" + server.getFirstMappedPort();
        logger.info("Elasticsearch " + version + ": " + url);
        for (int i = 0; i < 10; i++) {
            try (CurlResponse response = Curl.get(url).execute()) {
                if (response.getHttpStatusCode() == 200) {
                    logger.info(url + " is available.");
                    break;
                }
            } catch (final Exception e) {
                logger.fine(e.getLocalizedMessage());
            }
            try {
                logger.info("Waiting for " + url);
                Thread.sleep(1000L);
            } catch (final InterruptedException e) {
                // nothing
            }
        }
    }

    @BeforeEach
    void setUp() {
        final String host = server.getHost() + ":" + server.getFirstMappedPort();
        final Settings settings = Settings.builder().putList("http.hosts", host).put("http.compression", true).build();
        client = new HttpClient(settings, null);
    }

    @AfterEach
    void tearDown() {
        logger.info("Closing client");
        client.close();
    }

    @AfterAll
    static void tearDownAll() {
        server.stop();
    }

    @Test
    void test_refresh() throws Exception {
        final String index = "test_refresh";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareRefresh(index).execute(wrap(res -> {
            assertEquals(RestStatus.OK, res.getStatus());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final RefreshResponse refreshResponse = client.admin().indices().prepareRefresh(index).execute().actionGet();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
    }

    @Test
    void test_analyze() throws Exception {
        final String index = "test_analyze";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareAnalyze(index, "this is a pen.").setAnalyzer("standard").execute(wrap(res -> {
            assertEquals(4, res.getTokens().size());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final AnalyzeAction.Response analyzeResponse =
                    client.admin().indices().prepareAnalyze(index, "this is a pen.").setAnalyzer("standard").execute().actionGet();
            assertEquals(4, analyzeResponse.getTokens().size());
        }
    }

    @Test
    void test_search() throws Exception {
        final String index = "test_search";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute(wrap(res -> {
            assertEquals(0, res.getHits().getTotalHits().value());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            assertEquals(0, searchResponse.getHits().getTotalHits().value());
        }
    }

    @Test
    void test_create_index() throws Exception {
        final String index1 = "test_create_index1";
        final String index2 = "test_create_index2";
        final String index3 = "test_create_index3";
        final CountDownLatch latch = new CountDownLatch(1);

        client.admin().indices().prepareCreate(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final String settingsSource =
                    "{\"index\":{\"refresh_interval\":\"10s\",\"number_of_shards\":\"1\",\"auto_expand_replicas\":\"0-1\",\"number_of_replicas\":\"0\"}}";
            final String mappingSource = "{\"_source\":{\"includes\":[\"aaa\"],\"excludes\":[\"111\"]},"
                    //                                + "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"type\":\"keyword\"},\"match\":\"*\",\"match_mapping_type\":\"string\"}}],"
                    + "\"properties\":{\"@timestamp\":{\"type\":\"date\",\"format\":\"epoch_millis\"},\"docFreq\":{\"type\":\"long\"},\"fields\":{\"type\":\"keyword\"},\"kinds\":{\"type\":\"keyword\"},\"queryFreq\":{\"type\":\"long\"},\"roles\":{\"type\":\"keyword\"},\"languages\":{\"type\":\"keyword\"},\"score\":{\"type\":\"double\"},\"tags\":{\"type\":\"keyword\"},\"text\":{\"type\":\"keyword\"},\"userBoost\":{\"type\":\"double\"}}}";
            XContentHelper.convertToMap(new BytesArray(mappingSource), false, XContentType.JSON).v2();
            final CreateIndexResponse createIndexResponse =
                    client.admin().indices().prepareCreate(index2).setSettings(settingsSource, XContentType.JSON)//
                            .setMapping(mappingSource)//
                            //.addMapping("_doc", sourceMap)//
                            .addAlias(new Alias("fess.test2")).execute().actionGet();
            assertTrue(createIndexResponse.isAcknowledged());
            assertEquals(index2, createIndexResponse.index());
        }

        {
            final String source =
                    """
                            {"settings":\
                            {"index":{"refresh_interval":"10s","number_of_shards":"1","auto_expand_replicas":"0-1","number_of_replicas":"0"}}\
                            ,"mappings":{\
                            "_source":\
                            {"includes":["aaa"],"excludes":["111"]}\
                            ,"properties":\
                            {"@timestamp":{"type":"date","format":"epoch_millis"},"docFreq":{"type":"long"},"fields":{"type":"keyword"},"kinds":{"type":"keyword"},"queryFreq":{"type":"long"},"roles":{"type":"keyword"},"languages":{"type":"keyword"},"score":{"type":"double"},"tags":{"type":"keyword"},"text":{"type":"keyword"},"userBoost":{"type":"double"}}\
                            }}""";
            final CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(index3)
                    .setSource(source, XContentType.JSON).addAlias(new Alias("fess.test3")).execute().actionGet();
            assertTrue(createIndexResponse.isAcknowledged());
            assertEquals(index3, createIndexResponse.index());
        }
    }

    @Test
    void test_delete_index() throws Exception {
        final String index1 = "test_delete_index1";
        final String index2 = "test_delete_index2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareDelete(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            final AcknowledgedResponse deleteIndexResponse = client.admin().indices().prepareDelete(index2).execute().actionGet();
            assertTrue(deleteIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_get_index() throws Exception {
        final String index = "test_get_index";
        final String alias = "test_alias";
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//
                .startObject("test_prop")//
                .field("type", "text")//
                .endObject()//
                .endObject()//
                .endObject();
        final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
        client.admin().indices().preparePutMapping(index).setSource(source, XContentType.JSON).execute().actionGet();

        client.admin().indices().prepareGetIndex().addIndices(index).execute(wrap(res -> {
            try {
                assertEquals(index, res.getIndices()[0]);
                assertTrue(res.getAliases().containsKey(index));
                assertTrue(res.getMappings().containsKey(index)); // TODO properties
                assertTrue(res.getSettings().containsKey(index));
            } finally {
                latch.countDown();
            }
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().addIndices(index).execute().actionGet();
            assertEquals(index, getIndexResponse.getIndices()[0]);
            assertTrue(getIndexResponse.getAliases().containsKey(index));
            assertTrue(getIndexResponse.getMappings().containsKey("properties"));
            assertTrue(getIndexResponse.getSettings().containsKey(index));
        }
    }

    @Test
    void test_open_index() throws Exception {
        final String index1 = "test_open_index1";
        final String index2 = "test_open_index2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareOpen(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            final OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen(index2).execute().actionGet();
            assertTrue(openIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_close_index() throws Exception {
        final String index1 = "test_close_index1";
        final String index2 = "test_close_index2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareClose(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            final CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose(index2).execute().actionGet();
            assertTrue(closeIndexResponse.isAcknowledged());
            assertTrue(closeIndexResponse.isShardsAcknowledged());
            assertEquals(1, closeIndexResponse.getIndices().size());
        }
        {
            final CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose(index2).execute().actionGet();
            assertTrue(closeIndexResponse.isAcknowledged());
            assertFalse(closeIndexResponse.isShardsAcknowledged());
            assertEquals(0, closeIndexResponse.getIndices().size());
        }
    }

    @Test
    void test_indices_exists() throws Exception {
        final String index1 = "test_indices_exists1";
        final String index2 = "test_indices_exists2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareExists(index1).execute(wrap(res -> {
            assertTrue(res.isExists());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index1).execute().actionGet();
            assertTrue(indicesExistsResponse.isExists());
            indicesExistsResponse = client.admin().indices().prepareExists(index2).execute().actionGet();
            assertFalse(indicesExistsResponse.isExists());
        }
    }

    @Test
    void test_indices_aliases() throws Exception {
        final String index = "test_indices_aliases";
        final String alias1 = "test_alias1";
        final String alias2 = "test_alias2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareAliases().addAlias(index, alias1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final AcknowledgedResponse indicesAliasesResponse =
                    client.admin().indices().prepareAliases().addAlias(index, alias2).execute().actionGet();
            assertTrue(indicesAliasesResponse.isAcknowledged());
        }
    }

    @Test
    void test_put_mapping() throws Exception {
        final String index1 = "test_put_mapping1";
        final String index2 = "test_put_mapping2";
        final String index3 = "test_put_mapping3";
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("test_prop").field("type", "text").endObject().endObject().endObject();
        final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().preparePutMapping(index1).setSource(source, XContentType.JSON).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final String mappingSource =
                    "{\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"type\":\"keyword\"},\"match\":\"*\",\"match_mapping_type\":\"string\"}}],"
                            + "\"properties\":{\"@timestamp\":{\"type\":\"date\",\"format\":\"epoch_millis\"},\"docFreq\":{\"type\":\"long\"},\"fields\":{\"type\":\"keyword\"},\"kinds\":{\"type\":\"keyword\"},\"queryFreq\":{\"type\":\"long\"},\"roles\":{\"type\":\"keyword\"},\"languages\":{\"type\":\"keyword\"},\"score\":{\"type\":\"double\"},\"tags\":{\"type\":\"keyword\"},\"text\":{\"type\":\"keyword\"},\"userBoost\":{\"type\":\"double\"}}}";
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            final AcknowledgedResponse putMappingResponse =
                    client.admin().indices().preparePutMapping(index2).setSource(mappingSource, XContentType.JSON).execute().actionGet();
            assertTrue(putMappingResponse.isAcknowledged());
        }

        {
            client.admin().indices().prepareCreate(index3).execute().actionGet();
            final AcknowledgedResponse putMappingResponse = client
                    .admin().indices().preparePutMapping(index3).setSource(XContentFactory.jsonBuilder().startObject()
                            .startObject("properties").startObject("key").field("type", "keyword").endObject().endObject().endObject())
                    .execute().actionGet();
            assertTrue(putMappingResponse.isAcknowledged());
        }
    }

    @Test
    void test_get_mappings() throws Exception {
        final String index = "test_get_mappings1";

        try {
            client.admin().indices().prepareGetMappings("not_exists").execute().actionGet();
            fail();
        } catch (final IndexNotFoundException e) {
            // ok
        } catch (final Exception e) {
            e.printStackTrace();
            fail();
        }

        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("test_prop").field("type", "text").endObject().endObject().endObject();
        final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        XContentHelper.convertToMap(BytesReference.bytes(mappingBuilder), true, XContentType.JSON).v2();
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().preparePutMapping(index).setSource(source, XContentType.JSON).execute().actionGet();

        client.admin().indices().prepareGetMappings(index).execute(wrap(res -> {
            try {
                final Map<String, MappingMetadata> mappings = res.getMappings();
                assertTrue(mappings.containsKey("properties"));
            } finally {
                latch.countDown();
            }
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(index).execute().actionGet();
            final Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
            assertTrue(mappings.containsKey("properties"));
        }
    }

    @Test
    void test_flush() throws Exception {
        final String index = "test_flush";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareFlush(index).execute(wrap(res -> {
            assertEquals(RestStatus.OK, res.getStatus());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final FlushResponse res = client.admin().indices().prepareFlush(index).execute().actionGet();
            assertEquals(RestStatus.OK, res.getStatus());
        }
    }

    @Test
    void test_crud_index0() throws Exception {
        final String index = "test_crud_index";
        final String id = "1";

        // Get the document
        try {
            client.prepareGet().setIndex(index).setId(id).execute().actionGet();
            fail();
        } catch (final IndexNotFoundException e) {
            // ok
        }

        // Create a document
        final IndexResponse indexResponse = client.prepareIndex().setIndex(index).setId(id).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON)
                .execute().actionGet();
        assertTrue((Result.CREATED == indexResponse.getResult()) || (Result.UPDATED == indexResponse.getResult()));

        // Refresh index to search
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        // Search the document
        final SearchResponse searchResponse =
                client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).execute().actionGet();
        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        // Get the document
        final GetResponse getResponse2 = client.prepareGet().setIndex(index).setId(id).execute().actionGet();
        assertTrue(getResponse2.isExists());

        // Update the document
        final UpdateResponse updateResponse = client.prepareUpdate().setIndex(index).setId(id).setDoc("foo", "bar").execute().actionGet();
        assertEquals(Result.UPDATED, updateResponse.getResult());

        // Delete the document
        final DeleteResponse deleteResponse = client.prepareDelete().setIndex(index).setId(id).execute().actionGet();
        assertEquals(RestStatus.OK, deleteResponse.status());

        // make sure the document was deleted
        final GetResponse response = client.prepareGet().setIndex(index).setId(id).execute().actionGet();
        assertFalse(response.isExists());
    }

    @Test
    void test_get_settings() throws Exception {
        final String index = "test_get_settings";
        final String id = "1";
        final CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex().setIndex(index).setId(id).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON)
                .execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareGetSettings(index).execute(wrap(res -> {
            assertTrue(res.getSetting(index, "index.number_of_shards") != null);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings(index).execute().actionGet();
            assertTrue(getSettingsResponse.getSetting(index, "index.number_of_shards") != null);
        }
    }

    @Test
    void test_cluster_health() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        client.admin().cluster().prepareHealth().execute(wrap(res -> {
            try {
                assertEquals(res.getClusterName(), clusterName);
            } finally {
                latch.countDown();
            }
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            final ClusterHealthResponse custerHealthResponse = client.admin().cluster().prepareHealth().execute().actionGet();
            assertEquals(custerHealthResponse.getClusterName(), clusterName);
        }
    }

    @Test
    void test_get_aliases() throws Exception {
        final String index = "test_get_aliases";
        final String alias1 = "test_alias1";
        final String alias2 = "test_alias2";
        final CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().prepareAliases().addAlias(index, alias1).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareGetAliases().setIndices(index).setAliases(alias1).execute(wrap(res -> {
            assertTrue(res.getAliases().size() == 1);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            try {
                fail();
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

        {
            client.admin().indices().prepareAliases().addAlias(index, alias2).execute().actionGet();
            final GetAliasesResponse getAliasesResponse =
                    client.admin().indices().prepareGetAliases().setIndices(index).setAliases(alias1).execute().actionGet();
            assertTrue(getAliasesResponse.getAliases().size() == 1);
        }
    }

    @Test
    void test_stats() throws Exception {
        assertEquals("1.1.1.1:0", HttpNodesStatsAction.parseTransportAddress("1.1.1.1").toString());
        assertEquals("1.1.1.1:9300", HttpNodesStatsAction.parseTransportAddress("1.1.1.1:9300").toString());
        assertEquals("[::1]:0", HttpNodesStatsAction.parseTransportAddress("[::1]").toString());
        assertEquals("[::1]:9300", HttpNodesStatsAction.parseTransportAddress("[::1]:9300").toString());

        {
            final NodesStatsResponse response = client.admin().cluster().prepareNodesStats().execute().actionGet();
            assertFalse(response.getNodes().isEmpty());
        }

        {
            final NodesStatsResponse response = client.admin().cluster().prepareNodesStats()
                    .addMetrics("fs", "jvm", "os", "process", "thread_pool", "transport").execute().actionGet();
            assertFalse(response.getNodes().isEmpty());
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();
            try (OutputStream out = builder.getOutputStream()) {
                final String value = ((ByteArrayOutputStream) out).toString("UTF-8");
                System.out.println(value);
            }
        }
    }

    @Test
    void test_hotThreads() throws Exception {
        {
            final NodesHotThreadsResponse response = client.admin().cluster().prepareNodesHotThreads().execute().actionGet();
            assertFalse(response.getNodes().isEmpty());
            response.getNodes().forEach(node -> {
                System.out.println(node.getNode().toString() + "\n" + node.getHotThreads());
                assertNotNull(node.getNode());
                assertNotNull(node.getHotThreads());
            });
        }

        {
            final NodesHotThreadsResponse response = client.admin().cluster().prepareNodesHotThreads().setType("wait").setThreads(10)
                    .setTimeout("10s").setInterval(TimeValue.timeValueSeconds(5)).execute().actionGet();
            assertFalse(response.getNodes().isEmpty());
            response.getNodes().forEach(node -> {
                System.out.println(node.getNode().toString() + "\n" + node.getHotThreads());
                assertNotNull(node.getNode());
                assertNotNull(node.getHotThreads());
            });
        }
    }

    // TODO PutIndexTemplateAction
    // TODO GetIndexTemplatesAction
    // TODO DeleteIndexTemplateAction
    // TODO CancelTasksAction
    // TODO ListTasksAction
    // TODO VerifyRepositoryAction
    // TODO PutRepositoryAction
    // TODO GetRepositoriesAction
    // TODO DeleteRepositoryAction
    // TODO SnapshotsStatusAction
    // TODO CreateSnapshotAction
    // TODO GetSnapshotsAction
    // TODO DeleteSnapshotAction
    // TODO RestoreSnapshotAction

    @Test
    void test_pit_create_delete() throws Exception {
        final String index = "test_pit_create_delete";
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (int i = 1; i <= 3; i++) {
            bulkRequestBuilder.add(
                    client.prepareIndex().setIndex(index).setId(String.valueOf(i)).setSource("{\"value\":" + i + "}", XContentType.JSON));
        }
        bulkRequestBuilder.execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        final org.codelibs.fesen.opensearch.action.search.CreatePitRequest createPitRequest =
                new org.codelibs.fesen.opensearch.action.search.CreatePitRequest(TimeValue.timeValueMinutes(1), true, index);
        final org.codelibs.fesen.opensearch.action.search.CreatePitResponse createPitResponse =
                client.execute(org.codelibs.fesen.opensearch.action.search.CreatePitAction.INSTANCE, createPitRequest).actionGet();
        final String pitId = createPitResponse.getId();
        assertNotNull(pitId);
        assertFalse(pitId.isEmpty());

        final org.codelibs.fesen.opensearch.action.search.DeletePitResponse deletePitResponse =
                client.execute(org.codelibs.fesen.opensearch.action.search.DeletePitAction.INSTANCE,
                        new org.codelibs.fesen.opensearch.action.search.DeletePitRequest(pitId)).actionGet();
        assertTrue(deletePitResponse.getDeletePitResults().stream()
                .allMatch(org.codelibs.fesen.opensearch.action.search.DeletePitInfo::isSuccessful));
    }

    @Test
    void test_pit_delete_all_unsupported() throws Exception {
        assertThrows(UnsupportedOperationException.class,
                () -> client.execute(org.codelibs.fesen.opensearch.action.search.DeletePitAction.INSTANCE,
                        new org.codelibs.fesen.opensearch.action.search.DeletePitRequest("_all")).actionGet());
    }
}
