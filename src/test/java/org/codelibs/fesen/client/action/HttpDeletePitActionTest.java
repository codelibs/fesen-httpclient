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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpDeletePitActionTest {

    private final HttpDeletePitAction action = new HttpDeletePitAction(null, DeletePitAction.INSTANCE);

    private static HttpDeletePitAction action(final EngineType engineType) {
        return new HttpDeletePitAction(ActionTestUtils.testClient(engineType), DeletePitAction.INSTANCE);
    }

    // --- OpenSearch fromXContent (unchanged behavior) ---

    @Test
    void test_fromXContent() throws IOException {
        final String json = "{\"pits\":[{\"successful\":true,\"pit_id\":\"abc\"},{\"successful\":false,\"pit_id\":\"def\"}]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final DeletePitResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(2, response.getDeletePitResults().size());
            assertTrue(response.getDeletePitResults().get(0).isSuccessful());
            assertEquals("abc", response.getDeletePitResults().get(0).getPitId());
            assertEquals(false, response.getDeletePitResults().get(1).isSuccessful());
            assertEquals("def", response.getDeletePitResults().get(1).getPitId());
        }
    }

    // --- Per-engine request bodies ---

    @Test
    void test_buildBody_openSearch() {
        final DeletePitRequest request = new DeletePitRequest("a", "b");
        final String body = action(EngineType.OPENSEARCH3).buildBody(request);
        assertTrue(body.contains("\"pit_id\""), body);
        assertTrue(body.contains("\"a\""), body);
        assertTrue(body.contains("\"b\""), body);
    }

    @Test
    void test_buildEsBody_elasticsearch() {
        // The Elasticsearch _pit endpoint accepts a single id (a JSON string, not an array).
        final DeletePitRequest request = new DeletePitRequest("a");
        final String body = action(EngineType.ELASTICSEARCH8).buildEsBody(request);
        assertTrue(body.contains("\"id\""), body);
        assertFalse(body.contains("\"pit_id\""), body);
        assertTrue(body.contains("\"a\""), body);
        assertFalse(body.contains("["), body);
    }

    // --- Per-engine getCurlRequest endpoints ---

    @Test
    void test_getCurlRequest_openSearch() {
        final DeletePitRequest request = new DeletePitRequest("a", "b");
        final String url = ActionTestUtils.url(action(EngineType.OPENSEARCH3).getCurlRequest(request));
        assertTrue(url.endsWith("/_search/point_in_time"), url);
    }

    @Test
    void test_getCurlRequest_elasticsearch() {
        final DeletePitRequest request = new DeletePitRequest("a", "b");
        final String url = ActionTestUtils.url(action(EngineType.ELASTICSEARCH8).getCurlRequest(request));
        assertTrue(url.endsWith("/_pit"), url);
    }

    @Test
    void test_getCurlRequest_deleteAll_openSearch() {
        final DeletePitRequest request = new DeletePitRequest("_all");
        final String url = ActionTestUtils.url(action(EngineType.OPENSEARCH3).getCurlRequest(request));
        assertTrue(url.endsWith("/_search/point_in_time/_all"), url);
    }

    // --- Gating ---

    @Test
    void test_execute_unsupportedEngine() {
        // OpenSearch 1.x predates PIT; testClient cannot stub UNKNOWN (its stub falls back to OpenSearch 3.x).
        final AtomicReference<Exception> failure = new AtomicReference<>();
        action(EngineType.OPENSEARCH1).execute(new DeletePitRequest("a"), ActionListener.wrap(r -> {}, failure::set));
        assertInstanceOf(UnsupportedOperationException.class, failure.get());
    }

    @Test
    void test_execute_deleteAll_elasticsearchUnsupported() {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        action(EngineType.ELASTICSEARCH8).execute(new DeletePitRequest("_all"), ActionListener.wrap(r -> {}, failure::set));
        assertInstanceOf(UnsupportedOperationException.class, failure.get());
    }

    @Test
    void test_execute_multipleIds_elasticsearchUnsupported() {
        // Elasticsearch _pit deletes a single id per request; multiple ids are rejected up front.
        final AtomicReference<Exception> failure = new AtomicReference<>();
        action(EngineType.ELASTICSEARCH8).execute(new DeletePitRequest("a", "b"), ActionListener.wrap(r -> {}, failure::set));
        assertInstanceOf(UnsupportedOperationException.class, failure.get());
    }

    // --- Elasticsearch response parsing ---

    @Test
    void test_parseEsDelete() throws IOException {
        final String json = "{\"succeeded\":true,\"num_freed\":2}";
        final DeletePitRequest request = new DeletePitRequest("a", "b");
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final DeletePitResponse response = action.parseEsDelete(parser, request);
            assertNotNull(response);
            assertEquals(2, response.getDeletePitResults().size());
            assertTrue(response.getDeletePitResults().get(0).isSuccessful());
            assertTrue(response.getDeletePitResults().get(1).isSuccessful());
            assertEquals("a", response.getDeletePitResults().get(0).getPitId());
            assertEquals("b", response.getDeletePitResults().get(1).getPitId());
        }
    }

    @Test
    void test_parseEsDelete_errorBody_throws() throws IOException {
        // An error body (routed through the success path) must surface as a failure, not be reported
        // as an unsuccessful deletion of the requested ids.
        final String json = "{\"error\":{\"type\":\"illegal_argument_exception\"},\"status\":400}";
        final DeletePitRequest request = new DeletePitRequest("a");
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            assertThrows(IOException.class, () -> action.parseEsDelete(parser, request));
        }
    }
}
