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
package org.codelibs.fesen.client.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.pipeline.SearchPipelineStats;
import org.opensearch.tasks.TaskCancellationStats;

import sun.misc.Unsafe;

/**
 * Tests for {@link HttpNodesStatsAction} JSON parsing logic.
 * Uses Unsafe to instantiate without calling the constructor that requires HttpClient.
 */
class HttpNodesStatsActionTest {

    private static HttpNodesStatsAction action;

    @BeforeAll
    static void setUp() throws Exception {
        final Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        final Unsafe unsafe = (Unsafe) f.get(null);
        action = (HttpNodesStatsAction) unsafe.allocateInstance(HttpNodesStatsAction.class);
    }

    private XContentParser createParser(final String json) throws IOException {
        final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        parser.nextToken(); // advance to first token
        return parser;
    }

    // Helper methods to call protected methods via reflection
    private NodesStatsResponse callFromXContent(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("fromXContent", XContentParser.class);
        m.setAccessible(true);
        return (NodesStatsResponse) m.invoke(action, parser);
    }

    @SuppressWarnings("unchecked")
    private List<NodeStats> callParseNodes(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseNodes", XContentParser.class);
        m.setAccessible(true);
        return (List<NodeStats>) m.invoke(action, parser);
    }

    private NodeStats callParseNodeStats(final XContentParser parser, final String nodeId) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseNodeStats", XContentParser.class, String.class);
        m.setAccessible(true);
        return (NodeStats) m.invoke(action, parser, nodeId);
    }

    private SearchBackpressureStats callParseSearchBackpressureStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseSearchBackpressureStats", XContentParser.class);
        m.setAccessible(true);
        return (SearchBackpressureStats) m.invoke(action, parser);
    }

    private TaskCancellationStats callParseTaskCancellationStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseTaskCancellationStats", XContentParser.class);
        m.setAccessible(true);
        return (TaskCancellationStats) m.invoke(action, parser);
    }

    private SearchPipelineStats callParseSearchPipelineStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseSearchPipelineStats", XContentParser.class);
        m.setAccessible(true);
        return (SearchPipelineStats) m.invoke(action, parser);
    }

    private void callConsumeObject(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("consumeObject", XContentParser.class);
        m.setAccessible(true);
        try {
            m.invoke(action, parser);
        } catch (final java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    private Throwable invokeAndGetCause(final Runnable r) {
        try {
            r.run();
            return null;
        } catch (final Exception e) {
            return e;
        }
    }

    // ==================== fromXContent tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_minimalResponse() throws Exception {
        final String json = "{\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0},\"cluster_name\":\"test-cluster\",\"nodes\":{}}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals("test-cluster", response.getClusterName().value());
            assertEquals(0, response.getNodes().size());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_withSingleNode() throws Exception {
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"node1\":{" + "    \"name\":\"test-node\"," + "    \"timestamp\":1234567890,"
                + "    \"transport_address\":\"127.0.0.1:9300\"" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getNodes().size());
            assertEquals("test-node", response.getNodes().get(0).getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_withMultipleNodes() throws Exception {
        final String json = "{" + "\"_nodes\":{\"total\":2,\"successful\":2,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"node1\":{\"name\":\"node-A\",\"timestamp\":100},"
                + "  \"node2\":{\"name\":\"node-B\",\"timestamp\":200}" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(2, response.getNodes().size());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_withUnknownTopLevelFields() throws Exception {
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"cluster_uuid\":\"abc-123\"," + "\"nodes\":{}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals("test-cluster", response.getClusterName().value());
        }
    }

    // ==================== parseNodeStats with all new stats sections ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withSearchBackpressure() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"search_backpressure\":{"
                + "  \"search_task\":{\"cancellation_count\":5,\"limit_reached_count\":2,\"completion_count\":100},"
                + "  \"search_shard_task\":{\"cancellation_count\":3,\"limit_reached_count\":1,\"completion_count\":50},"
                + "  \"mode\":\"monitor_only\"" + "}," + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken(); // advance into the object
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withTaskCancellation() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"task_cancellation\":{"
                + "  \"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},"
                + "  \"search_shard_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":20}" + "},"
                + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withSearchPipeline() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"search_pipeline\":{"
                + "  \"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},"
                + "  \"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0}," + "  \"pipelines\":{}" + "},"
                + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withSegmentReplicationBackpressure() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"segment_replication_backpressure\":{"
                + "  \"total_rejected_requests\":42" + "}," + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withAdmissionControlAndCaches() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"admission_control\":{"
                + "  \"global_cpu_usage\":{\"transport\":{\"rejection_count\":{\"current_rejections\":0}}}" + "}," + "\"caches\":{"
                + "  \"request_cache\":{\"size_in_bytes\":1024,\"evictions\":5}" + "}," + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withRemoteStore() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"remote_store\":{"
                + "  \"last_successful_fetch_of_pinned_timestamps\":0" + "}," + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withRoles() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890,"
                + "\"roles\":[\"data\",\"ingest\",\"cluster_manager\"]," + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertTrue(nodeStats.getNode().getRoles().size() > 0);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withUnknownObjectFields() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"future_unknown_stat\":{"
                + "  \"nested\":{\"deep\":{\"value\":123}}," + "  \"array_field\":[1,2,3]" + "},"
                + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_withUnknownArrayFields() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"future_array_stat\":[{\"a\":1},{\"b\":2}],"
                + "\"transport_address\":\"127.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    // ==================== parseSearchBackpressureStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_full() throws Exception {
        final String json = "{" + "\"search_task\":{\"cancellation_count\":5,\"limit_reached_count\":2,\"completion_count\":100},"
                + "\"search_shard_task\":{\"cancellation_count\":3,\"limit_reached_count\":1,\"completion_count\":50},"
                + "\"mode\":\"monitor_only\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_withUnknownFields() throws Exception {
        final String json = "{" + "\"search_task\":{\"cancellation_count\":5}," + "\"search_shard_task\":{\"cancellation_count\":3},"
                + "\"mode\":\"monitor_only\"," + "\"future_field\":{\"nested\":true}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
        }
    }

    // ==================== parseTaskCancellationStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_full() throws Exception {
        final String json = "{" + "\"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},"
                + "\"search_shard_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":20}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final TaskCancellationStats stats = callParseTaskCancellationStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_withUnknownFields() throws Exception {
        final String json = "{" + "\"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},"
                + "\"search_shard_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":20},"
                + "\"future_task\":{\"count\":5}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final TaskCancellationStats stats = callParseTaskCancellationStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final TaskCancellationStats stats = callParseTaskCancellationStats(parser);
            assertNotNull(stats);
        }
    }

    // ==================== parseSearchPipelineStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_full() throws Exception {
        final String json = "{" + "\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},"
                + "\"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0}," + "\"pipelines\":{}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_withUnknownObjectAndArray() throws Exception {
        final String json = "{" + "\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},"
                + "\"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0},"
                + "\"pipelines\":{\"my_pipeline\":{\"request_count\":5}}," + "\"processors\":[{\"type\":\"rename\",\"count\":3}]" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
        }
    }

    // ==================== consumeObject tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_simpleObject() throws Exception {
        final String json = "{\"a\":1,\"b\":\"hello\",\"c\":true}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken(); // advance into object
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_nestedObjects() throws Exception {
        final String json = "{\"a\":{\"b\":{\"c\":{\"d\":1}}},\"e\":2}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_withArrays() throws Exception {
        final String json = "{\"arr\":[1,2,3],\"nested_arr\":[[1,2],[3,4]],\"obj_arr\":[{\"x\":1},{\"y\":2}]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_nullTokenThrowsIOException() throws Exception {
        // Truncated JSON - should throw IOException instead of spinning
        final String json = "{\"a\":1";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(IOException.class, () -> callConsumeObject(parser));
        }
    }

    // ==================== Full node stats response (integration-like) ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_withAllNewFieldsTogether() throws Exception {
        // Test that all newly added field parsers work together without consuming each other's tokens
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"node1\":{" + "    \"name\":\"test-node\"," + "    \"timestamp\":1234567890,"
                + "    \"transport_address\":\"127.0.0.1:9300\"," + "    \"search_backpressure\":{"
                + "      \"search_task\":{\"cancellation_count\":5}," + "      \"search_shard_task\":{\"cancellation_count\":3},"
                + "      \"mode\":\"monitor_only\"" + "    }," + "    \"cluster_manager_throttling\":{}," + "    \"task_cancellation\":{"
                + "      \"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":5},"
                + "      \"search_shard_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":3}" + "    },"
                + "    \"search_pipeline\":{"
                + "      \"total_request\":{\"count\":100,\"time_in_millis\":5000,\"current\":0,\"failed\":2},"
                + "      \"total_response\":{\"count\":98,\"time_in_millis\":4000,\"current\":0,\"failed\":0}" + "    },"
                + "    \"segment_replication_backpressure\":{\"total_rejected_requests\":0},"
                + "    \"admission_control\":{\"global_cpu_usage\":{}}," + "    \"caches\":{\"request_cache\":{}},"
                + "    \"remote_store\":{}" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getNodes().size());
            assertEquals("test-node", response.getNodes().get(0).getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_doesNotHangOnComplexNestedUnknownFields() throws Exception {
        // Simulate a future OpenSearch version with many unknown nested fields
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"node1\":{" + "    \"name\":\"test-node\"," + "    \"timestamp\":1234567890,"
                + "    \"transport_address\":\"127.0.0.1:9300\","
                + "    \"unknown_stat_1\":{\"deep\":{\"deeper\":{\"deepest\":[1,2,{\"x\":3}]}}},"
                + "    \"unknown_stat_2\":{\"array_of_objects\":[{\"a\":1},{\"b\":2},{\"c\":[3,4,5]}]},"
                + "    \"unknown_stat_3\":{\"mixed\":{\"num\":1,\"str\":\"hello\",\"bool\":true,\"null_val\":null,\"arr\":[],\"obj\":{}}},"
                + "    \"unknown_array\":[1,\"two\",true,null,[],{}]" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getNodes().size());
            assertEquals("test-node", response.getNodes().get(0).getNode().getName());
        }
    }

    // ==================== Runaway protection tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_truncatedJsonThrowsException() throws Exception {
        // Truncated JSON - parser will reach EOF before END_OBJECT
        final String json = "{\"cluster_name\":\"test\"";
        try (final XContentParser parser = createParser(json)) {
            assertThrows(Exception.class, () -> callFromXContent(parser));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodes_truncatedJsonThrowsException() throws Exception {
        final String json = "{\"node1\":{\"name\":\"test\"";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(Exception.class, () -> callParseNodes(parser));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_truncatedJsonThrowsException() throws Exception {
        final String json = "{\"name\":\"test\",\"timestamp\":123";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(Exception.class, () -> callParseNodeStats(parser, "node1"));
        }
    }

    // ==================== Token boundary verification ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_parserEndsAtEndObject() throws Exception {
        // After parsing, parser must be at END_OBJECT - not past it
        final String json = "{\"search_task\":{\"cancellation_count\":5},\"search_shard_task\":{},\"mode\":\"monitor_only\"}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callParseSearchBackpressureStats(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_parserEndsAtEndObject() throws Exception {
        final String json =
                "{\"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},\"search_shard_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callParseTaskCancellationStats(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_parserEndsAtEndObject() throws Exception {
        final String json =
                "{\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},\"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callParseSearchPipelineStats(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== Stat section followed by scalar field verification ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_searchBackpressureFollowedByScalarFields() throws Exception {
        // The original bug: search_backpressure consumed tokens of following fields
        final String json = "{" + "\"search_backpressure\":{" + "  \"search_task\":{\"cancellation_count\":5,\"limit_reached_count\":2},"
                + "  \"search_shard_task\":{\"cancellation_count\":3}," + "  \"mode\":\"monitor_only\"" + "},"
                + "\"name\":\"verify-after-backpressure\"," + "\"timestamp\":9999," + "\"transport_address\":\"10.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("verify-after-backpressure", nodeStats.getNode().getName());
            assertEquals(9999, nodeStats.getTimestamp());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_taskCancellationFollowedByScalarFields() throws Exception {
        final String json =
                "{" + "\"task_cancellation\":{" + "  \"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},"
                        + "  \"search_shard_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":20}" + "},"
                        + "\"name\":\"verify-after-cancellation\"," + "\"timestamp\":8888" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("verify-after-cancellation", nodeStats.getNode().getName());
            assertEquals(8888, nodeStats.getTimestamp());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_searchPipelineFollowedByScalarFields() throws Exception {
        final String json =
                "{" + "\"search_pipeline\":{" + "  \"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},"
                        + "  \"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0}," + "  \"pipelines\":{}"
                        + "}," + "\"name\":\"verify-after-pipeline\"," + "\"timestamp\":7777" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("verify-after-pipeline", nodeStats.getNode().getName());
            assertEquals(7777, nodeStats.getTimestamp());
        }
    }

    // ==================== Stat section as first/last field ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_searchBackpressureAsLastField() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"search_backpressure\":{"
                + "  \"search_task\":{\"cancellation_count\":5}," + "  \"search_shard_task\":{\"cancellation_count\":3},"
                + "  \"mode\":\"monitor_only\"" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_taskCancellationAsFirstField() throws Exception {
        final String json =
                "{" + "\"task_cancellation\":{" + "  \"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":5}" + "},"
                        + "\"name\":\"test-node\"," + "\"timestamp\":1234567890" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
            assertEquals(1234567890, nodeStats.getTimestamp());
        }
    }

    // ==================== Multiple stat sections in sequence ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_allThreeStatSectionsInSequence() throws Exception {
        // The critical regression test: all three fixed methods in sequence
        final String json = "{" + "\"name\":\"seq-node\"," + "\"timestamp\":111,"
                + "\"search_backpressure\":{\"search_task\":{\"a\":1,\"b\":2},\"search_shard_task\":{\"c\":3},\"mode\":\"monitor_only\"},"
                + "\"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10},\"search_shard_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":20}},"
                + "\"search_pipeline\":{\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},\"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0}},"
                + "\"transport_address\":\"10.0.0.1:9300\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("seq-node", nodeStats.getNode().getName());
            assertEquals(111, nodeStats.getTimestamp());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_statSectionsFollowedByAnotherStatSection() throws Exception {
        // search_backpressure followed by segment_replication_backpressure
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890,"
                + "\"search_backpressure\":{\"search_task\":{\"x\":1},\"search_shard_task\":{\"y\":2},\"mode\":\"monitor_only\"},"
                + "\"segment_replication_backpressure\":{\"total_rejected_requests\":42},"
                + "\"admission_control\":{\"global_cpu_usage\":{\"transport\":{\"rejection_count\":{}}}},"
                + "\"caches\":{\"request_cache\":{\"size_in_bytes\":0}}," + "\"remote_store\":{\"stat\":1}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals("test-node", nodeStats.getNode().getName());
        }
    }

    // ==================== Multiple nodes with stat sections ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_multipleNodesWithStatSections() throws Exception {
        // Verify correct node count when each node has stat sections
        final String json = "{" + "\"_nodes\":{\"total\":3,\"successful\":3,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"n1\":{\"name\":\"node-1\",\"timestamp\":100,"
                + "    \"search_backpressure\":{\"search_task\":{},\"search_shard_task\":{},\"mode\":\"monitor_only\"},"
                + "    \"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0}}" + "  },"
                + "  \"n2\":{\"name\":\"node-2\",\"timestamp\":200,"
                + "    \"search_pipeline\":{\"total_request\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0},\"total_response\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0}}"
                + "  }," + "  \"n3\":{\"name\":\"node-3\",\"timestamp\":300,"
                + "    \"search_backpressure\":{\"search_task\":{\"a\":1},\"search_shard_task\":{\"b\":2},\"mode\":\"enforced\"},"
                + "    \"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":5,\"total_count_post_cancel\":50}},"
                + "    \"search_pipeline\":{\"total_request\":{\"count\":100,\"time_in_millis\":500,\"current\":1,\"failed\":0},\"total_response\":{\"count\":99,\"time_in_millis\":400,\"current\":0,\"failed\":1}},"
                + "    \"segment_replication_backpressure\":{\"total_rejected_requests\":10},"
                + "    \"admission_control\":{\"policy1\":{\"enabled\":true}},"
                + "    \"caches\":{\"request_cache\":{\"size_in_bytes\":1024}}," + "    \"remote_store\":{\"data\":{}}" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(3, response.getNodes().size());
            assertEquals("node-1", response.getNodes().get(0).getNode().getName());
            assertEquals("node-2", response.getNodes().get(1).getNode().getName());
            assertEquals("node-3", response.getNodes().get(2).getNode().getName());
        }
    }

    // ==================== Partial sub-fields in stat sections ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_onlySearchTask() throws Exception {
        final String json = "{\"search_task\":{\"cancellation_count\":5,\"limit_reached_count\":2,\"completion_count\":100}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_onlyMode() throws Exception {
        final String json = "{\"mode\":\"enforced\"}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_onlySearchTask() throws Exception {
        final String json = "{\"search_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":10}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final TaskCancellationStats stats = callParseTaskCancellationStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_onlyTotalRequest() throws Exception {
        final String json = "{\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== Deeply nested objects inside stat sections ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_deeplyNestedSearchTask() throws Exception {
        final String json = "{" + "\"search_task\":{\"cancellation_count\":5,\"resource_tracker\":{"
                + "  \"heap_usage\":{\"current_max\":1024,\"rolling_avg\":512,\"cancellation_count\":1},"
                + "  \"cpu_usage\":{\"current_max\":80,\"rolling_avg\":50,\"cancellation_count\":0},"
                + "  \"elapsed_time\":{\"current_max\":30000,\"rolling_avg\":15000,\"cancellation_count\":2}" + "}},"
                + "\"search_shard_task\":{\"cancellation_count\":3,\"resource_tracker\":{"
                + "  \"heap_usage\":{\"current_max\":2048,\"rolling_avg\":1024}" + "}}," + "\"mode\":\"enforced\"" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_deeplyNestedPipelines() throws Exception {
        final String json = "{" + "\"total_request\":{\"count\":10,\"time_in_millis\":100,\"current\":0,\"failed\":1},"
                + "\"total_response\":{\"count\":9,\"time_in_millis\":90,\"current\":0,\"failed\":0},"
                + "\"pipelines\":{\"my_pipeline\":{\"request\":{\"count\":5,\"time_in_millis\":50,\"current\":0,\"failed\":0},"
                + "\"response\":{\"count\":5,\"time_in_millis\":40,\"current\":0,\"failed\":0},"
                + "\"request_processors\":[{\"filter:abc\":{\"type\":\"filter\",\"stats\":{\"count\":5,\"time_in_millis\":10,\"current\":0,\"failed\":0}}}],"
                + "\"response_processors\":[]}}}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== consumeObject edge cases ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_mixedNestedArraysAndObjects() throws Exception {
        // Complex real-world-like structure
        final String json =
                "{\"level1\":{\"arr\":[{\"inner\":{\"deep\":[1,2,3]}},{\"other\":[]}],\"sibling\":true},\"top_arr\":[[[1]],{\"a\":{\"b\":[{\"c\":1}]}}]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_onlyNestedArrays() throws Exception {
        final String json = "{\"a\":[1,2],\"b\":[[3,4],[5,6]],\"c\":[{\"x\":[7]}]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_consumeObject_allValueTypes() throws Exception {
        final String json = "{\"num\":42,\"float\":3.14,\"str\":\"hello\",\"bool\":true,\"nil\":null,\"neg\":-1}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            callConsumeObject(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== Truncated JSON inside stat sections ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_truncatedInsideSearchBackpressure() throws Exception {
        final String json = "{\"name\":\"test\",\"search_backpressure\":{\"search_task\":{\"a\":1";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(Exception.class, () -> callParseNodeStats(parser, "node1"));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_truncatedInsideTaskCancellation() throws Exception {
        final String json = "{\"name\":\"test\",\"task_cancellation\":{\"search_task\":{\"x\":1";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(Exception.class, () -> callParseNodeStats(parser, "node1"));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_truncatedInsideSearchPipeline() throws Exception {
        final String json = "{\"name\":\"test\",\"search_pipeline\":{\"total_request\":{\"count\":10";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            assertThrows(Exception.class, () -> callParseNodeStats(parser, "node1"));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_truncatedInsideNodeStats() throws Exception {
        final String json =
                "{\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0},\"cluster_name\":\"c\",\"nodes\":{\"n1\":{\"name\":\"n\",\"search_backpressure\":{\"search_task\":{";
        try (final XContentParser parser = createParser(json)) {
            assertThrows(Exception.class, () -> callFromXContent(parser));
        }
    }

    // ==================== Stat section with unknown sub-fields of various types ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchBackpressureStats_withArraySubField() throws Exception {
        final String json =
                "{\"search_task\":{\"a\":1},\"search_shard_task\":{\"b\":2},\"mode\":\"monitor_only\",\"future_array\":[1,2,3]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchBackpressureStats stats = callParseSearchBackpressureStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseTaskCancellationStats_withArraySubField() throws Exception {
        final String json = "{\"search_task\":{\"x\":1},\"future_array\":[{\"a\":1},{\"b\":2}]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final TaskCancellationStats stats = callParseTaskCancellationStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseSearchPipelineStats_withNestedArray() throws Exception {
        final String json =
                "{\"total_request\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0},\"total_response\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0},\"pipelines\":{},\"future\":[{\"nested\":{\"deep\":[1,2]}}]}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final SearchPipelineStats stats = callParseSearchPipelineStats(parser);
            assertNotNull(stats);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== Realistic OpenSearch _nodes/stats response ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_fromXContent_realisticOpenSearch3Response() throws Exception {
        // Simulate a realistic OpenSearch 3.x _nodes/stats response
        final String json = "{" + "\"_nodes\":{\"total\":2,\"successful\":2,\"failed\":0}," + "\"cluster_name\":\"production-cluster\","
                + "\"nodes\":{" + "  \"abc123\":{\"name\":\"data-node-1\",\"timestamp\":1711100000000,"
                + "    \"transport_address\":\"10.0.1.1:9300\"," + "    \"roles\":[\"data\",\"ingest\"],"
                // Note: indices section omitted as it requires ClusterSettings for SearchRequestStats
                + "    \"process\":{\"timestamp\":1711100000000,\"open_file_descriptors\":300,\"max_file_descriptors\":65536,\"cpu\":{\"percent\":15,\"total_in_millis\":500000},\"mem\":{\"total_virtual_in_bytes\":8000000000}},"
                + "    \"jvm\":{\"timestamp\":1711100000000,\"uptime_in_millis\":86400000,\"mem\":{\"heap_used_in_bytes\":2000000000,\"heap_used_percent\":62,\"heap_committed_in_bytes\":3200000000,\"heap_max_in_bytes\":3200000000,\"non_heap_used_in_bytes\":200000000,\"non_heap_committed_in_bytes\":250000000},\"threads\":{\"count\":100,\"peak_count\":120},\"gc\":{\"collectors\":{\"young\":{\"collection_count\":500,\"collection_time_in_millis\":15000},\"old\":{\"collection_count\":5,\"collection_time_in_millis\":3000}}},\"classes\":{\"current_loaded_count\":15000,\"total_loaded_count\":15000,\"total_unloaded_count\":0},\"buffer_pools\":{\"mapped\":{\"count\":50,\"used_in_bytes\":4000000000,\"total_capacity_in_bytes\":4000000000},\"direct\":{\"count\":100,\"used_in_bytes\":500000000,\"total_capacity_in_bytes\":500000000}}},"
                + "    \"thread_pool\":{\"search\":{\"threads\":13,\"queue\":0,\"active\":3,\"rejected\":0,\"largest\":13,\"completed\":50000,\"total_wait_time_in_nanos\":0},\"write\":{\"threads\":4,\"queue\":0,\"active\":1,\"rejected\":0,\"largest\":4,\"completed\":100000,\"total_wait_time_in_nanos\":0}},"
                + "    \"transport\":{\"server_open\":10,\"total_outbound_connections\":20,\"rx_count\":500000,\"rx_size_in_bytes\":2000000000,\"tx_count\":500000,\"tx_size_in_bytes\":2000000000},"
                + "    \"http\":{\"current_open\":50,\"total_opened\":10000},"
                + "    \"search_backpressure\":{\"search_task\":{\"cancellation_count\":10,\"limit_reached_count\":5,\"completion_count\":49990},\"search_shard_task\":{\"cancellation_count\":3,\"limit_reached_count\":1,\"completion_count\":200000},\"mode\":\"enforced\"},"
                + "    \"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":10},\"search_shard_task\":{\"current_count_post_cancel\":1,\"total_count_post_cancel\":3}},"
                + "    \"search_pipeline\":{\"total_request\":{\"count\":50000,\"time_in_millis\":250000,\"current\":3,\"failed\":10},\"total_response\":{\"count\":49990,\"time_in_millis\":200000,\"current\":0,\"failed\":5}},"
                + "    \"segment_replication_backpressure\":{\"total_rejected_requests\":0},"
                + "    \"admission_control\":{\"global_cpu_usage\":{\"transport\":{\"rejection_count\":{\"current_rejections\":0,\"total_rejections\":5}}}},"
                + "    \"caches\":{\"request_cache\":{\"size_in_bytes\":104857600,\"evictions\":100}},"
                + "    \"remote_store\":{\"last_successful_fetch_of_pinned_timestamps\":1711099999000}" + "  },"
                + "  \"def456\":{\"name\":\"data-node-2\",\"timestamp\":1711100000000," + "    \"transport_address\":\"10.0.1.2:9300\","
                + "    \"roles\":[\"data\",\"cluster_manager\"],"
                + "    \"search_backpressure\":{\"search_task\":{\"cancellation_count\":2},\"search_shard_task\":{\"cancellation_count\":1},\"mode\":\"monitor_only\"},"
                + "    \"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":2}},"
                + "    \"search_pipeline\":{\"total_request\":{\"count\":1000,\"time_in_millis\":5000,\"current\":0,\"failed\":0},\"total_response\":{\"count\":1000,\"time_in_millis\":4000,\"current\":0,\"failed\":0}}"
                + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals("production-cluster", response.getClusterName().value());
            assertEquals(2, response.getNodes().size());
            assertEquals("data-node-1", response.getNodes().get(0).getNode().getName());
            assertEquals("data-node-2", response.getNodes().get(1).getNode().getName());
            // Verify individual stats were parsed correctly
            final NodeStats node1 = response.getNodes().get(0);
            assertNotNull(node1.getProcess());
            assertNotNull(node1.getJvm());
            assertNotNull(node1.getTransport());
            assertNotNull(node1.getHttp());
            assertTrue(node1.getNode().getRoles().size() > 0);
            assertTrue(response.getNodes().get(1).getNode().getRoles().size() > 0);
        }
    }

    // ==================== New helper methods for new parser methods ====================

    private BlockCacheStats callParseBlockCacheStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseBlockCacheStats", XContentParser.class);
        m.setAccessible(true);
        return (BlockCacheStats) m.invoke(action, parser);
    }

    private AnalyticsBackendNativeMemoryStats callParseAnalyticsBackendNativeMemoryStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseAnalyticsBackendNativeMemoryStats", XContentParser.class);
        m.setAccessible(true);
        return (AnalyticsBackendNativeMemoryStats) m.invoke(action, parser);
    }

    private NativeAllocatorPoolStats callParseNativeAllocatorPoolStats(final XContentParser parser) throws Exception {
        final Method m = HttpNodesStatsAction.class.getDeclaredMethod("parseNativeAllocatorPoolStats", XContentParser.class);
        m.setAccessible(true);
        return (NativeAllocatorPoolStats) m.invoke(action, parser);
    }

    // ==================== parseNativeAllocatorPoolStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNativeAllocatorPoolStats_fullParse() throws Exception {
        final String json = "{" + "\"root\":{\"allocated_bytes\":10,\"peak_bytes\":20,\"limit_bytes\":30}," + "\"pools\":{"
                + "  \"pool-a\":{\"allocated_bytes\":1,\"peak_bytes\":2,\"limit_bytes\":3},"
                + "  \"pool-b\":{\"allocated_bytes\":4,\"peak_bytes\":5,\"limit_bytes\":6}" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken(); // advance past START_OBJECT into first field
            final NativeAllocatorPoolStats stats = callParseNativeAllocatorPoolStats(parser);
            assertNotNull(stats);
            assertEquals(10L, stats.getRootAllocatedBytes());
            assertEquals(20L, stats.getRootPeakBytes());
            assertEquals(30L, stats.getRootLimitBytes());
            assertEquals(2, stats.getPools().size());
            final NativeAllocatorPoolStats.PoolStats poolA = stats.getPools().get(0);
            assertEquals("pool-a", poolA.getName());
            assertEquals(1L, poolA.getAllocatedBytes());
            assertEquals(2L, poolA.getPeakBytes());
            assertEquals(3L, poolA.getLimitBytes());
            final NativeAllocatorPoolStats.PoolStats poolB = stats.getPools().get(1);
            assertEquals("pool-b", poolB.getName());
            assertEquals(4L, poolB.getAllocatedBytes());
            assertEquals(5L, poolB.getPeakBytes());
            assertEquals(6L, poolB.getLimitBytes());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNativeAllocatorPoolStats_noPools() throws Exception {
        final String json = "{\"root\":{\"allocated_bytes\":100,\"peak_bytes\":200,\"limit_bytes\":300}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NativeAllocatorPoolStats stats = callParseNativeAllocatorPoolStats(parser);
            assertNotNull(stats);
            assertEquals(100L, stats.getRootAllocatedBytes());
            assertEquals(200L, stats.getRootPeakBytes());
            assertEquals(300L, stats.getRootLimitBytes());
            assertNotNull(stats.getPools());
            assertEquals(0, stats.getPools().size());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== parseAnalyticsBackendNativeMemoryStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseAnalyticsBackendNativeMemoryStats_full() throws Exception {
        final String json = "{\"allocated_bytes\":1111,\"resident_bytes\":2222}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final AnalyticsBackendNativeMemoryStats stats = callParseAnalyticsBackendNativeMemoryStats(parser);
            assertNotNull(stats);
            assertEquals(1111L, stats.getAllocatedBytes());
            assertEquals(2222L, stats.getResidentBytes());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseAnalyticsBackendNativeMemoryStats_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final AnalyticsBackendNativeMemoryStats stats = callParseAnalyticsBackendNativeMemoryStats(parser);
            assertNotNull(stats);
            assertEquals(0L, stats.getAllocatedBytes());
            assertEquals(0L, stats.getResidentBytes());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== parseBlockCacheStats tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseBlockCacheStats_full() throws Exception {
        final String json = "{" + "\"over_all_stats\":{\"active_in_bytes\":800,\"used_in_bytes\":900,\"pinned_in_bytes\":0,"
                + "  \"evictions_in_bytes\":50,\"removed_in_bytes\":30,\"active_percent\":0,\"hit_count\":500,\"miss_count\":100},"
                + "\"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "  \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "\"block_file_stats\":{\"active_in_bytes\":800,\"used_in_bytes\":900,\"pinned_in_bytes\":0,"
                + "  \"evictions_in_bytes\":50,\"removed_in_bytes\":30,\"active_percent\":0,\"hit_count\":500,\"miss_count\":100},"
                + "\"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "  \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final BlockCacheStats stats = callParseBlockCacheStats(parser);
            assertNotNull(stats);
            assertEquals(500L, stats.hits());
            assertEquals(100L, stats.misses());
            assertEquals(50L, stats.evictionBytes());
            assertEquals(30L, stats.removedBytes());
            assertEquals(800L, stats.activeInBytes());
            assertEquals(900L, stats.memoryBytesUsed());
            assertEquals(0L, stats.hitBytes());
            assertEquals(0L, stats.missBytes());
            assertEquals(0L, stats.evictions());
            assertEquals(0L, stats.removed());
            assertEquals(0L, stats.diskBytesUsed());
            assertEquals(0L, stats.totalBytes());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseBlockCacheStats_empty() throws Exception {
        final String json = "{}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final BlockCacheStats stats = callParseBlockCacheStats(parser);
            assertNotNull(stats);
            assertEquals(0L, stats.hits());
            assertEquals(0L, stats.misses());
            assertEquals(0L, stats.hitBytes());
            assertEquals(0L, stats.missBytes());
            assertEquals(0L, stats.evictions());
            assertEquals(0L, stats.evictionBytes());
            assertEquals(0L, stats.removed());
            assertEquals(0L, stats.removedBytes());
            assertEquals(0L, stats.memoryBytesUsed());
            assertEquals(0L, stats.diskBytesUsed());
            assertEquals(0L, stats.totalBytes());
            assertEquals(0L, stats.activeInBytes());
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        }
    }

    // ==================== parseNodeStats: native_memory tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_nativeMemoryFull() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":1234567890," + "\"native_memory\":{"
                + "  \"total_estimated_bytes\":123456," + "  \"analytics_backend\":{\"allocated_bytes\":1111,\"resident_bytes\":2222},"
                + "  \"native_allocator\":{" + "    \"root\":{\"allocated_bytes\":10,\"peak_bytes\":20,\"limit_bytes\":30},"
                + "    \"pools\":{" + "      \"pool-a\":{\"allocated_bytes\":1,\"peak_bytes\":2,\"limit_bytes\":3},"
                + "      \"pool-b\":{\"allocated_bytes\":4,\"peak_bytes\":5,\"limit_bytes\":6}" + "    }" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals(123456L, nodeStats.getTotalEstimatedNativeBytes());
            final AnalyticsBackendNativeMemoryStats nativeMemory = nodeStats.getAnalyticsBackendNativeMemoryStats();
            assertNotNull(nativeMemory);
            assertEquals(1111L, nativeMemory.getAllocatedBytes());
            assertEquals(2222L, nativeMemory.getResidentBytes());
            final NativeAllocatorPoolStats nativeAllocator = nodeStats.getNativeAllocatorStats();
            assertNotNull(nativeAllocator);
            assertEquals(10L, nativeAllocator.getRootAllocatedBytes());
            assertEquals(20L, nativeAllocator.getRootPeakBytes());
            assertEquals(30L, nativeAllocator.getRootLimitBytes());
            assertEquals(2, nativeAllocator.getPools().size());
            final NativeAllocatorPoolStats.PoolStats pool0 = nativeAllocator.getPools().get(0);
            assertEquals("pool-a", pool0.getName());
            assertEquals(1L, pool0.getAllocatedBytes());
            assertEquals(2L, pool0.getPeakBytes());
            assertEquals(3L, pool0.getLimitBytes());
            final NativeAllocatorPoolStats.PoolStats pool1 = nativeAllocator.getPools().get(1);
            assertEquals("pool-b", pool1.getName());
            assertEquals(4L, pool1.getAllocatedBytes());
            assertEquals(5L, pool1.getPeakBytes());
            assertEquals(6L, pool1.getLimitBytes());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_nativeMemoryMinimal() throws Exception {
        final String json =
                "{" + "\"name\":\"test-node\"," + "\"timestamp\":9999," + "\"native_memory\":{\"total_estimated_bytes\":99999}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals(99999L, nodeStats.getTotalEstimatedNativeBytes());
            assertNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
            assertNull(nodeStats.getNativeAllocatorStats());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_nativeMemoryAbsent() throws Exception {
        final String json = "{\"name\":\"test-node\",\"timestamp\":111}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals(-1L, nodeStats.getTotalEstimatedNativeBytes());
            assertNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
            assertNull(nodeStats.getNativeAllocatorStats());
        }
    }

    // ==================== parseNodeStats: file cache dispatch tests ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_aggregateFileCache_populatesFileCacheStats() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":111," + "\"aggregate_file_cache\":{"
                + "  \"timestamp\":123456789," + "  \"active_in_bytes\":100,\"total_in_bytes\":200,\"used_in_bytes\":150,"
                + "  \"pinned_in_bytes\":10,\"evictions_in_bytes\":5,\"removed_in_bytes\":3,"
                + "  \"active_percent\":50,\"used_percent\":75,\"hit_count\":42,\"miss_count\":7,"
                + "  \"over_all_stats\":{\"active_in_bytes\":100,\"used_in_bytes\":150,\"pinned_in_bytes\":10,"
                + "    \"evictions_in_bytes\":5,\"removed_in_bytes\":3,\"active_percent\":50,\"hit_count\":42,\"miss_count\":7},"
                + "  \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "  \"block_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "  \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertNotNull(nodeStats.getFileCacheStats());
            assertEquals(123456789L, nodeStats.getFileCacheStats().getTimestamp());
            assertEquals(42L, nodeStats.getFileCacheStats().getCacheHits());
            assertEquals(7L, nodeStats.getFileCacheStats().getCacheMisses());
            assertNull(nodeStats.getFileCacheOnlyStats());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_fileCacheDetailed_populatesFileCacheOnlyStats() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":222," + "\"file_cache\":{"
                + "  \"over_all_stats\":{\"active_in_bytes\":100,\"used_in_bytes\":150,\"pinned_in_bytes\":10,"
                + "    \"evictions_in_bytes\":5,\"removed_in_bytes\":3,\"active_percent\":50,\"hit_count\":42,\"miss_count\":7},"
                + "  \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "  \"block_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "  \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertNotNull(nodeStats.getFileCacheOnlyStats());
            assertEquals(42L, nodeStats.getFileCacheOnlyStats().getCacheHits());
            assertEquals(7L, nodeStats.getFileCacheOnlyStats().getCacheMisses());
            assertNull(nodeStats.getFileCacheStats());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_blockCache_populatesBlockCacheOnlyStats() throws Exception {
        final String json = "{" + "\"name\":\"test-node\"," + "\"timestamp\":333," + "\"block_cache\":{"
                + "  \"over_all_stats\":{\"active_in_bytes\":800,\"used_in_bytes\":900,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":50,\"removed_in_bytes\":30,\"active_percent\":0,\"hit_count\":500,\"miss_count\":100},"
                + "  \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "  \"block_file_stats\":{\"active_in_bytes\":800,\"used_in_bytes\":900,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":50,\"removed_in_bytes\":30,\"active_percent\":0,\"hit_count\":500,\"miss_count\":100},"
                + "  \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "    \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            final BlockCacheStats blockCache = nodeStats.getBlockCacheOnlyStats();
            assertNotNull(blockCache);
            assertEquals(500L, blockCache.hits());
            assertEquals(100L, blockCache.misses());
            assertEquals(50L, blockCache.evictionBytes());
            assertEquals(30L, blockCache.removedBytes());
            assertEquals(800L, blockCache.activeInBytes());
            assertEquals(900L, blockCache.memoryBytesUsed());
            assertEquals(0L, blockCache.hitBytes());
            assertEquals(0L, blockCache.missBytes());
            assertEquals(0L, blockCache.evictions());
            assertEquals(0L, blockCache.removed());
            assertEquals(0L, blockCache.diskBytesUsed());
            assertEquals(0L, blockCache.totalBytes());
        }
    }

    // ==================== Integration-style test: all new sections together ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_allNewSectionsTogether_withJvmAndOs() throws Exception {
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{\"node1\":{" + "  \"name\":\"test-node\",\"timestamp\":9999," + "  \"transport_address\":\"127.0.0.1:9300\","
                + "  \"jvm\":{" + "    \"timestamp\":9999,\"uptime_in_millis\":1000," + "    \"mem\":{"
                + "      \"heap_used_in_bytes\":1000,\"heap_used_percent\":10,"
                + "      \"heap_committed_in_bytes\":2000,\"heap_max_in_bytes\":2000,"
                + "      \"non_heap_used_in_bytes\":100,\"non_heap_committed_in_bytes\":150" + "    },"
                + "    \"threads\":{\"count\":10,\"peak_count\":15},"
                + "    \"gc\":{\"collectors\":{\"young\":{\"collection_count\":5,\"collection_time_in_millis\":100},"
                + "      \"old\":{\"collection_count\":1,\"collection_time_in_millis\":500}}},"
                + "    \"classes\":{\"current_loaded_count\":100,\"total_loaded_count\":100,\"total_unloaded_count\":0},"
                + "    \"buffer_pools\":{}" + "  }," + "  \"transport\":{\"server_open\":5,\"total_outbound_connections\":3,"
                + "    \"rx_count\":100,\"rx_size_in_bytes\":5000,\"tx_count\":100,\"tx_size_in_bytes\":5000},"
                + "  \"aggregate_file_cache\":{" + "    \"timestamp\":11111,"
                + "    \"over_all_stats\":{\"active_in_bytes\":10,\"used_in_bytes\":20,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":1,\"removed_in_bytes\":0,\"active_percent\":50,\"hit_count\":5,\"miss_count\":2},"
                + "    \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"block_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "  },"
                + "  \"file_cache\":{" + "    \"over_all_stats\":{\"active_in_bytes\":10,\"used_in_bytes\":20,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":1,\"removed_in_bytes\":0,\"active_percent\":50,\"hit_count\":5,\"miss_count\":2},"
                + "    \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"block_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "  },"
                + "  \"block_cache\":{" + "    \"over_all_stats\":{\"active_in_bytes\":800,\"used_in_bytes\":900,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":50,\"removed_in_bytes\":30,\"active_percent\":0,\"hit_count\":500,\"miss_count\":100},"
                + "    \"full_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"block_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0},"
                + "    \"pinned_file_stats\":{\"active_in_bytes\":0,\"used_in_bytes\":0,\"pinned_in_bytes\":0,"
                + "      \"evictions_in_bytes\":0,\"removed_in_bytes\":0,\"active_percent\":0,\"hit_count\":0,\"miss_count\":0}" + "  },"
                + "  \"native_memory\":{" + "    \"total_estimated_bytes\":123456,"
                + "    \"analytics_backend\":{\"allocated_bytes\":1111,\"resident_bytes\":2222}," + "    \"native_allocator\":{"
                + "      \"root\":{\"allocated_bytes\":10,\"peak_bytes\":20,\"limit_bytes\":30},"
                + "      \"pools\":{\"pool-a\":{\"allocated_bytes\":1,\"peak_bytes\":2,\"limit_bytes\":3}}" + "    }" + "  }" + "}}" + "}";
        try (final XContentParser parser = createParser(json)) {
            final NodesStatsResponse response = callFromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getNodes().size());
            final NodeStats node = response.getNodes().get(0);
            assertEquals("test-node", node.getNode().getName());
            assertNotNull(node.getFileCacheStats());
            assertNotNull(node.getFileCacheOnlyStats());
            assertNotNull(node.getBlockCacheOnlyStats());
            assertEquals(123456L, node.getTotalEstimatedNativeBytes());
            assertNotNull(node.getAnalyticsBackendNativeMemoryStats());
            assertNotNull(node.getNativeAllocatorStats());
            assertNotNull(node.getJvm());
            assertNotNull(node.getTransport());
        }
    }

    // ==================== Robustness: unknown nested fields in native_memory ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_nativeMemory_unknownNestedObject_consumedGracefully() throws Exception {
        final String json = "{" + "\"name\":\"test-node\",\"timestamp\":111," + "\"native_memory\":{" + "  \"total_estimated_bytes\":500,"
                + "  \"future_field\":{\"nested\":{\"deep\":true},\"count\":42},"
                + "  \"analytics_backend\":{\"allocated_bytes\":10,\"resident_bytes\":20}," + "  \"native_allocator\":{"
                + "    \"root\":{\"allocated_bytes\":1,\"peak_bytes\":2,\"limit_bytes\":3}," + "    \"pools\":{}" + "  }" + "}" + "}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals(500L, nodeStats.getTotalEstimatedNativeBytes());
            assertNotNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
            assertEquals(10L, nodeStats.getAnalyticsBackendNativeMemoryStats().getAllocatedBytes());
            assertNotNull(nodeStats.getNativeAllocatorStats());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_nativeMemory_emptyObject() throws Exception {
        final String json = "{\"name\":\"test-node\",\"timestamp\":111,\"native_memory\":{}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            assertEquals(-1L, nodeStats.getTotalEstimatedNativeBytes());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_parseNodeStats_blockCache_emptyObject() throws Exception {
        final String json = "{\"name\":\"test-node\",\"timestamp\":111,\"block_cache\":{}}";
        try (final XContentParser parser = createParser(json)) {
            parser.nextToken();
            final NodeStats nodeStats = callParseNodeStats(parser, "node1");
            assertNotNull(nodeStats);
            final BlockCacheStats blockCache = nodeStats.getBlockCacheOnlyStats();
            assertNotNull(blockCache);
            assertEquals(0L, blockCache.hits());
            assertEquals(0L, blockCache.misses());
            assertEquals(0L, blockCache.evictionBytes());
            assertEquals(0L, blockCache.removedBytes());
            assertEquals(0L, blockCache.activeInBytes());
            assertEquals(0L, blockCache.memoryBytesUsed());
        }
    }

    // ==================== Concurrent parsing test ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void test_fromXContent_noInfiniteLoopWithConcurrentParsing() throws Exception {
        final String json = "{" + "\"_nodes\":{\"total\":1,\"successful\":1,\"failed\":0}," + "\"cluster_name\":\"test-cluster\","
                + "\"nodes\":{" + "  \"node1\":{" + "    \"name\":\"test-node\"," + "    \"timestamp\":1234567890,"
                + "    \"search_backpressure\":{\"search_task\":{},\"search_shard_task\":{},\"mode\":\"monitor_only\"},"
                + "    \"task_cancellation\":{\"search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0},\"search_shard_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0}},"
                + "    \"search_pipeline\":{\"total_request\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0},\"total_response\":{\"count\":0,\"time_in_millis\":0,\"current\":0,\"failed\":0}}"
                + "  }" + "}" + "}";

        final int threadCount = 5;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try (final XContentParser parser = createParser(json)) {
                    final NodesStatsResponse response = callFromXContent(parser);
                    assertNotNull(response);
                    assertEquals(1, response.getNodes().size());
                } catch (final Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(8, TimeUnit.SECONDS), "Parsing should complete within timeout");
        executor.shutdownNow();
        if (error.get() != null) {
            fail("Concurrent parsing failed: " + error.get().getMessage());
        }
    }

    // ==================== getCurlRequest: detailed=true query param ====================

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_getCurlRequest_fileCacheDetailed_true_addsDetailedParam() throws Exception {
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9201").build();
        try (final HttpClient httpClient = new HttpClient(settings, null)) {
            final HttpNodesStatsAction nodesStatsAction = new HttpNodesStatsAction(httpClient, NodesStatsAction.INSTANCE);
            final NodesStatsRequest request = new NodesStatsRequest();
            request.fileCacheDetailed(true);
            final CurlRequest curlRequest = nodesStatsAction.getCurlRequest(request);
            final Field paramListField = CurlRequest.class.getDeclaredField("paramList");
            paramListField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final List<String> paramList = (List<String>) paramListField.get(curlRequest);
            assertNotNull(paramList, "paramList must not be null when detailed=true is set");
            assertTrue(paramList.contains("detailed=true"), "paramList must contain 'detailed=true', actual: " + paramList);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test_getCurlRequest_fileCacheDetailed_false_omitsDetailedParam() throws Exception {
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9201").build();
        try (final HttpClient httpClient = new HttpClient(settings, null)) {
            final HttpNodesStatsAction nodesStatsAction = new HttpNodesStatsAction(httpClient, NodesStatsAction.INSTANCE);
            final NodesStatsRequest request = new NodesStatsRequest(); // default: fileCacheDetailed == false
            final CurlRequest curlRequest = nodesStatsAction.getCurlRequest(request);
            final Field paramListField = CurlRequest.class.getDeclaredField("paramList");
            paramListField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final List<String> paramList = (List<String>) paramListField.get(curlRequest);
            // paramList may be null (no params set at all) or non-null but must not contain "detailed=true"
            assertTrue(paramList == null || !paramList.contains("detailed=true"),
                    "paramList must not contain 'detailed=true', actual: " + paramList);
        }
    }
}
