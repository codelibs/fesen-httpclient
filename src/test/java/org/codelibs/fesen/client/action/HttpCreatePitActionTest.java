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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpCreatePitActionTest {

    private final HttpCreatePitAction action = new HttpCreatePitAction(null, CreatePitAction.INSTANCE);

    private static HttpCreatePitAction action(final EngineType engineType) {
        return new HttpCreatePitAction(ActionTestUtils.testClient(engineType), CreatePitAction.INSTANCE);
    }

    private static CreatePitRequest request() {
        return new CreatePitRequest(TimeValue.timeValueMinutes(5), true, "idx");
    }

    // --- OpenSearch fromXContent (unchanged behavior) ---

    @Test
    void test_fromXContent() throws IOException {
        final String json = "{\"pit_id\":\"abc\",\"_shards\":{\"total\":2,\"successful\":2,\"skipped\":0,\"failed\":0},"
                + "\"creation_time\":1720000000000}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final CreatePitResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals("abc", response.getId());
            assertEquals(1720000000000L, response.getCreationTime());
            assertEquals(2, response.getTotalShards());
            assertEquals(2, response.getSuccessfulShards());
            assertEquals(0, response.getSkippedShards());
            assertEquals(0, response.getFailedShards());
        }
    }

    // --- Per-engine getCurlRequest ---

    @Test
    void test_getCurlRequest_openSearch() {
        for (final EngineType engineType : new EngineType[] { EngineType.OPENSEARCH2, EngineType.OPENSEARCH3 }) {
            final HttpCreatePitAction osAction = action(engineType);
            final CreatePitRequest request = request();
            final var curlRequest = osAction.getCurlRequest(request);
            assertTrue(ActionTestUtils.url(curlRequest).endsWith("/idx/_search/point_in_time"), engineType.toString());
            final Map<String, String> params = ActionTestUtils.params(curlRequest);
            assertEquals("true", params.get("allow_partial_pit_creation"), engineType.toString());
            assertEquals("5m", params.get("keep_alive"), engineType.toString());
            assertFalse(params.containsKey("allow_partial_search_results"), engineType.toString());
        }
    }

    @Test
    void test_getCurlRequest_elasticsearch() {
        for (final EngineType engineType : new EngineType[] { EngineType.ELASTICSEARCH7, EngineType.ELASTICSEARCH8 }) {
            final HttpCreatePitAction esAction = action(engineType);
            final CreatePitRequest request = request();
            final var curlRequest = esAction.getCurlRequest(request);
            assertTrue(ActionTestUtils.url(curlRequest).endsWith("/idx/_pit"), engineType.toString());
            final Map<String, String> params = ActionTestUtils.params(curlRequest);
            assertEquals("5m", params.get("keep_alive"), engineType.toString());
            // The Elasticsearch _pit endpoint rejects both partial-results parameters (HTTP 400),
            // so neither is sent.
            assertFalse(params.containsKey("allow_partial_search_results"), engineType.toString());
            assertFalse(params.containsKey("allow_partial_pit_creation"), engineType.toString());
        }
    }

    // --- Gating ---

    @Test
    void test_execute_unsupportedEngine() {
        // OpenSearch 1.x predates PIT; testClient cannot stub UNKNOWN (its stub falls back to OpenSearch 3.x).
        final HttpCreatePitAction gatedAction = action(EngineType.OPENSEARCH1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        gatedAction.execute(request(), ActionListener.wrap(r -> {}, failure::set));
        assertInstanceOf(UnsupportedOperationException.class, failure.get());
    }

    // --- Elasticsearch response parsing ---

    @Test
    void test_parseEsCreate_idOnly() throws IOException {
        final String json = "{\"id\":\"abc\"}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final CreatePitResponse response = action.parseEsCreate(parser);
            assertNotNull(response);
            assertEquals("abc", response.getId());
            assertEquals(0, response.getTotalShards());
            assertEquals(0, response.getSuccessfulShards());
            assertEquals(0, response.getSkippedShards());
            assertEquals(0, response.getFailedShards());
        }
    }

    @Test
    void test_parseEsCreate_withShards() throws IOException {
        final String json = "{\"id\":\"abc\",\"_shards\":{\"total\":5,\"successful\":5,\"skipped\":0,\"failed\":0}}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final CreatePitResponse response = action.parseEsCreate(parser);
            assertNotNull(response);
            assertEquals("abc", response.getId());
            assertEquals(5, response.getTotalShards());
            assertEquals(5, response.getSuccessfulShards());
            assertEquals(0, response.getSkippedShards());
            assertEquals(0, response.getFailedShards());
        }
    }

    @Test
    void test_parseEsCreate_errorBody_throws() throws IOException {
        // An error body (routed through the success path) must surface as a failure, not become a
        // response with a null id.
        final String json = "{\"error\":{\"type\":\"illegal_argument_exception\"},\"status\":400}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            assertThrows(IOException.class, () -> action.parseEsCreate(parser));
        }
    }
}
