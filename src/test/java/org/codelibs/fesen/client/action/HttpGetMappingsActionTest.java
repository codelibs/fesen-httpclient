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

import java.io.IOException;
import java.util.Map;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpGetMappingsActionTest {

    private final HttpGetMappingsAction os3Action =
            new HttpGetMappingsAction(ActionTestUtils.testClient(EngineType.OPENSEARCH3), GetMappingsAction.INSTANCE);

    private final HttpGetMappingsAction es8Action =
            new HttpGetMappingsAction(ActionTestUtils.testClient(EngineType.ELASTICSEARCH8), GetMappingsAction.INSTANCE);

    @Test
    void test_getCurlRequest_localPresentOnOpenSearch3() {
        final GetMappingsRequest request = new GetMappingsRequest().indices("test-index");
        final Map<String, String> params = ActionTestUtils.params(os3Action.getCurlRequest(request));
        // The index is carried in the path, and the mapping endpoint is used.
        assertTrue(ActionTestUtils.url(os3Action.getCurlRequest(request)).contains("/test-index/_mapping"));
        // local is emitted for every engine except Elasticsearch 8.x.
        assertTrue(params.containsKey("local"));
        assertEquals(Boolean.toString(request.local()), params.get("local"));
    }

    @Test
    void test_getCurlRequest_localAbsentOnElasticsearch8() {
        final GetMappingsRequest request = new GetMappingsRequest().indices("test-index");
        final Map<String, String> params = ActionTestUtils.params(es8Action.getCurlRequest(request));
        assertTrue(ActionTestUtils.url(es8Action.getCurlRequest(request)).contains("/test-index/_mapping"));
        // Elasticsearch 8.x rejects the local parameter on the mapping endpoint, so the action suppresses it.
        assertFalse(params.containsKey("local"));
        // Indices options are still forwarded on Elasticsearch 8.x.
        assertTrue(params.containsKey("ignore_unavailable"));
        assertTrue(params.containsKey("allow_no_indices"));
        assertTrue(params.containsKey("expand_wildcards"));
    }

    @Test
    void test_getCurlRequest_indicesOptionsForwarded() {
        final GetMappingsRequest request = new GetMappingsRequest().indices("test-index");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, false, true));
        final Map<String, String> params = ActionTestUtils.params(os3Action.getCurlRequest(request));
        assertEquals("true", params.get("ignore_unavailable"));
        assertEquals("false", params.get("allow_no_indices"));
        assertEquals("closed", params.get("expand_wildcards"));
    }

    @Test
    void test_fromXContent_skipsDynamicTemplates() throws IOException {
        // OpenSearch mapping response: properties plus a dynamic_templates block that must be skipped.
        final String json = "{\"test-index\":{\"mappings\":{" //
                + "\"properties\":{\"field1\":{\"type\":\"text\"}}," //
                + "\"dynamic_templates\":[{\"strings\":{\"match_mapping_type\":\"string\"," //
                + "\"mapping\":{\"type\":\"keyword\"}}}]}}}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final GetMappingsResponse response = HttpGetMappingsAction.fromXContent(parser);
            assertNotNull(response);
            final Map<String, MappingMetadata> mappings = response.mappings();
            // dynamic_templates is skipped for compatibility with older versions.
            assertFalse(mappings.containsKey("dynamic_templates"));
            assertEquals(1, mappings.size());
        }
    }
}
