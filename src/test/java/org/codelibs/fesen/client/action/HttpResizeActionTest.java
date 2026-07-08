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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.shrink.ResizeResponse;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpResizeActionTest {

    private final HttpResizeAction action = new HttpResizeAction(null, ResizeAction.INSTANCE);

    private final HttpResizeAction clientAction = new HttpResizeAction(ActionTestUtils.testClient(), ResizeAction.INSTANCE);

    private static ResizeRequest resizeRequest(final ResizeType type) {
        final ResizeRequest request = new ResizeRequest("target", "source");
        request.setResizeType(type);
        return request;
    }

    @Test
    void test_getCurlRequest_endpoint_perType() {
        assertTrue(ActionTestUtils.url(clientAction.getCurlRequest(resizeRequest(ResizeType.SHRINK))).contains("/source/_shrink/target"));
        assertTrue(ActionTestUtils.url(clientAction.getCurlRequest(resizeRequest(ResizeType.SPLIT))).contains("/source/_split/target"));
        assertTrue(ActionTestUtils.url(clientAction.getCurlRequest(resizeRequest(ResizeType.CLONE))).contains("/source/_clone/target"));
    }

    @Test
    void test_getCurlRequest_defaultOmitsWaitForActiveShards() {
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(resizeRequest(ResizeType.CLONE)));
        assertFalse(params.containsKey("wait_for_active_shards"));
        // copy_settings is only ever true and must not be emitted.
        assertFalse(params.containsKey("copy_settings"));
    }

    @Test
    void test_getCurlRequest_waitForActiveShards() {
        final ResizeRequest request = resizeRequest(ResizeType.CLONE);
        request.getTargetIndexRequest().waitForActiveShards(ActiveShardCount.ALL);
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("all", params.get("wait_for_active_shards"));
    }

    @Test
    void test_toSource_carriesSettings() {
        final ResizeRequest request = resizeRequest(ResizeType.SPLIT);
        request.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4));
        final String body = clientAction.toSource(request);
        assertTrue(body.contains("\"settings\""), body);
        assertTrue(body.contains("number_of_shards"), body);
    }

    @Test
    void test_fromXContent_success() throws IOException {
        final String json = "{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"target\"}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final ResizeResponse response = ResizeResponse.fromXContent(parser);
            assertTrue(response.isAcknowledged());
            assertTrue(response.isShardsAcknowledged());
            assertEquals("target", response.index());
        }
    }

    @Test
    void test_fromXContent_errorBody_throws() throws IOException {
        // A non-2xx error body is routed through the success path; the parser must reject it so the
        // failure surfaces instead of a false-positive response.
        final String json = "{\"error\":{\"type\":\"illegal_argument_exception\"},\"status\":400}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            assertThrows(Exception.class, () -> ResizeResponse.fromXContent(parser));
        }
    }
}
