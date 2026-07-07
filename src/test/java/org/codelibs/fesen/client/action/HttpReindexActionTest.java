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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequest;

class HttpReindexActionTest {

    private final HttpReindexAction action = new HttpReindexAction(null, ReindexAction.INSTANCE);

    private final HttpReindexAction clientAction = new HttpReindexAction(ActionTestUtils.testClient(), ReindexAction.INSTANCE);

    private static ReindexRequest reindexRequest() {
        final ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("src");
        request.setDestIndex("dst");
        return request;
    }

    @Test
    void test_getCurlRequest_endpointAndCommonParams() {
        final ReindexRequest request = reindexRequest();
        request.setRefresh(true);
        request.setSlices(3);
        request.setRequestsPerSecond(500f);
        request.setWaitForActiveShards(2);
        request.setMaxDocs(25);
        request.setAbortOnVersionConflict(false);
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        // Endpoint is POST /_reindex with no index in the path.
        assertTrue(ActionTestUtils.url(clientAction.getCurlRequest(request)).endsWith("/_reindex"));
        // Critical: wait_for_completion is always forced so the body can be parsed synchronously.
        assertEquals("true", params.get("wait_for_completion"));
        assertEquals("true", params.get("refresh"));
        assertEquals("3", params.get("slices"));
        assertEquals(Float.toString(request.getRequestsPerSecond()), params.get("requests_per_second"));
        assertEquals("2", params.get("wait_for_active_shards"));
        // For reindex, max_docs and conflicts ride in the BODY, never as query parameters.
        assertFalse(params.containsKey("max_docs"));
        assertFalse(params.containsKey("conflicts"));
    }

    @Test
    void test_getCurlRequest_body_carriesSourceDestMaxDocsConflicts() {
        final ReindexRequest request = reindexRequest();
        request.setMaxDocs(25);
        request.setAbortOnVersionConflict(false);
        final String body = clientAction.toSource(request);
        assertTrue(body.contains("\"source\""));
        assertTrue(body.contains("src"));
        assertTrue(body.contains("\"dest\""));
        assertTrue(body.contains("dst"));
        assertTrue(body.contains("\"max_docs\""));
        assertTrue(body.contains("\"conflicts\""));
    }

    @Test
    void test_getCurlRequest_waitForCompletionAlwaysTrueAndNoLeakedParams() {
        final ReindexRequest request = reindexRequest();
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("true", params.get("wait_for_completion"));
        // Optional parameters are omitted when unset.
        assertFalse(params.containsKey("refresh"));
        assertFalse(params.containsKey("slices"));
        assertFalse(params.containsKey("requests_per_second"));
        assertFalse(params.containsKey("wait_for_active_shards"));
        assertFalse(params.containsKey("conflicts"));
        assertFalse(params.containsKey("max_docs"));
    }

    @Test
    void test_fromXContent() throws Exception {
        final String json = "{\"took\":147,\"timed_out\":false,\"total\":120,\"updated\":0,\"created\":120,\"deleted\":0,"
                + "\"batches\":1,\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,\"search\":0},"
                + "\"throttled_millis\":0,\"requests_per_second\":-1.0,\"throttled_until_millis\":0,\"failures\":[]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkByScrollResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(147L, response.getTook().millis());
            assertFalse(response.isTimedOut());
            assertEquals(120L, response.getTotal());
            assertEquals(120L, response.getCreated());
            assertEquals(0L, response.getUpdated());
            assertEquals(0L, response.getDeleted());
            assertEquals(1, response.getBatches());
            assertEquals(0L, response.getVersionConflicts());
            assertEquals(0L, response.getNoops());
            assertEquals(0L, response.getBulkRetries());
            assertEquals(0L, response.getSearchRetries());
            assertEquals(0, response.getBulkFailures().size());
        }
    }
}
