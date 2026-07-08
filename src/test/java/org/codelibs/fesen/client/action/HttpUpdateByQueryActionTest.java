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
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;

class HttpUpdateByQueryActionTest {

    private final HttpUpdateByQueryAction action = new HttpUpdateByQueryAction(null, UpdateByQueryAction.INSTANCE);

    private final HttpUpdateByQueryAction clientAction =
            new HttpUpdateByQueryAction(ActionTestUtils.testClient(), UpdateByQueryAction.INSTANCE);

    @Test
    void test_getCurlRequest_endpointAndParams() {
        final UpdateByQueryRequest request = new UpdateByQueryRequest("idx");
        request.setAbortOnVersionConflict(false);
        request.setMaxDocs(50);
        request.setSlices(4);
        request.setRequestsPerSecond(250f);
        request.setRouting("r1");
        request.setPipeline("p1");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        // Endpoint is POST /idx/_update_by_query.
        assertTrue(ActionTestUtils.url(clientAction.getCurlRequest(request)).contains("/idx/_update_by_query"));
        assertEquals("true", params.get("wait_for_completion"));
        // For by-query, conflicts and max_docs are query PARAMETERS (unlike reindex, where they ride in the body).
        assertEquals("proceed", params.get("conflicts"));
        assertEquals("50", params.get("max_docs"));
        assertEquals("4", params.get("slices"));
        assertEquals(Float.toString(request.getRequestsPerSecond()), params.get("requests_per_second"));
        assertEquals("r1", params.get("routing"));
        assertEquals("p1", params.get("pipeline"));
    }

    @Test
    void test_getCurlRequest_defaultsOmitOptionalParams() {
        final UpdateByQueryRequest request = new UpdateByQueryRequest("idx");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("true", params.get("wait_for_completion"));
        // conflicts defaults to abort, so it is not emitted; the remaining optionals stay unset.
        assertFalse(params.containsKey("conflicts"));
        assertFalse(params.containsKey("max_docs"));
        assertFalse(params.containsKey("slices"));
        assertFalse(params.containsKey("routing"));
        assertFalse(params.containsKey("pipeline"));
    }

    @Test
    void test_fromXContent() throws Exception {
        final String json = "{\"took\":320,\"timed_out\":false,\"total\":100,\"updated\":80,\"created\":0,\"deleted\":0,"
                + "\"batches\":2,\"version_conflicts\":3,\"noops\":17,\"retries\":{\"bulk\":1,\"search\":0},"
                + "\"throttled_millis\":0,\"requests_per_second\":-1.0,\"throttled_until_millis\":0,\"failures\":[]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkByScrollResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(320L, response.getTook().millis());
            assertFalse(response.isTimedOut());
            assertEquals(100L, response.getTotal());
            assertEquals(80L, response.getUpdated());
            assertEquals(0L, response.getCreated());
            assertEquals(0L, response.getDeleted());
            assertEquals(2, response.getBatches());
            assertEquals(3L, response.getVersionConflicts());
            assertEquals(17L, response.getNoops());
            assertEquals(1L, response.getBulkRetries());
            assertEquals(0L, response.getSearchRetries());
            assertEquals(0, response.getBulkFailures().size());
        }
    }
}
