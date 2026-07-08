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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpGetAllPitsActionTest {

    private final HttpGetAllPitsAction action = new HttpGetAllPitsAction(null, GetAllPitsAction.INSTANCE);

    private static HttpGetAllPitsAction action(final EngineType engineType) {
        return new HttpGetAllPitsAction(ActionTestUtils.testClient(engineType), GetAllPitsAction.INSTANCE);
    }

    // --- OpenSearch fromXContent (unchanged behavior) ---

    @Test
    void test_fromXContent() throws IOException {
        final String json = "{\"pits\":[{\"pit_id\":\"abc\",\"creation_time\":123,\"keep_alive\":300000}]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final GetAllPitNodesResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getPitInfos().size());
            assertEquals("abc", response.getPitInfos().get(0).getPitId());
            assertEquals(123L, response.getPitInfos().get(0).getCreationTime());
            assertEquals(300000L, response.getPitInfos().get(0).getKeepAlive());
        }
    }

    // --- Per-engine getCurlRequest ---

    @Test
    void test_getCurlRequest_openSearch() {
        for (final EngineType engineType : new EngineType[] { EngineType.OPENSEARCH2, EngineType.OPENSEARCH3 }) {
            final String url = ActionTestUtils.url(action(engineType).getCurlRequest(new GetAllPitNodesRequest()));
            assertTrue(url.endsWith("/_search/point_in_time/_all"), engineType + ": " + url);
        }
    }

    // --- Gating ---

    @Test
    void test_execute_unsupportedEngine() {
        // Listing all PITs is OpenSearch 2.x+ only; testClient cannot stub UNKNOWN (its stub falls back to OpenSearch 3.x).
        for (final EngineType engineType : new EngineType[] { EngineType.OPENSEARCH1, EngineType.ELASTICSEARCH7,
                EngineType.ELASTICSEARCH8 }) {
            final AtomicReference<Exception> failure = new AtomicReference<>();
            action(engineType).execute(new GetAllPitNodesRequest(), ActionListener.wrap(r -> {}, failure::set));
            assertInstanceOf(UnsupportedOperationException.class, failure.get(), engineType.toString());
        }
    }
}
