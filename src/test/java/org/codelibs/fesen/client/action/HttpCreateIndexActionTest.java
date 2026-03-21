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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

class HttpCreateIndexActionTest {

    @Test
    void test_innerToXContent_withNoMappingsSet() throws IOException {
        final HttpCreateIndexAction action = new HttpCreateIndexAction(null, CreateIndexAction.INSTANCE);
        final CreateIndexRequest request = new CreateIndexRequest("test-index");
        // Don't set any mappings

        // Should NOT throw any exception (e.g., UnsupportedOperationException)
        final String result = assertDoesNotThrow(() -> {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            action.innerToXContent(request, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        });
        assertTrue(result.contains("settings"));
        assertTrue(result.contains("aliases"));
    }

    @Test
    void test_innerToXContent_withNullMappings() throws IOException {
        final HttpCreateIndexAction action = new HttpCreateIndexAction(null, CreateIndexAction.INSTANCE);
        // Use a subclass to force mappings() to return null
        final CreateIndexRequest request = new CreateIndexRequest("test-index") {
            @Override
            public String mappings() {
                return null;
            }
        };

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        action.innerToXContent(request, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        final String result = BytesReference.bytes(builder).utf8ToString();
        assertTrue(result.contains("settings"));
        assertTrue(result.contains("aliases"));
        // With null mappings, no mappings section should appear
        assertFalse(result.contains("mappings"));
    }

    @Test
    void test_innerToXContent_withMappings() throws IOException {
        final HttpCreateIndexAction action = new HttpCreateIndexAction(null, CreateIndexAction.INSTANCE);
        final CreateIndexRequest request = new CreateIndexRequest("test-index");
        request.mapping("{\"properties\":{\"field1\":{\"type\":\"text\"}}}");

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        action.innerToXContent(request, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        final String result = BytesReference.bytes(builder).utf8ToString();
        assertTrue(result.contains("settings"));
        assertTrue(result.contains("aliases"));
        assertTrue(result.contains("mappings"));
        assertTrue(result.contains("properties"));
        assertTrue(result.contains("field1"));
    }

    @Test
    void test_innerToXContent_withDocWrapperMappings() throws IOException {
        final HttpCreateIndexAction action = new HttpCreateIndexAction(null, CreateIndexAction.INSTANCE);
        final CreateIndexRequest request = new CreateIndexRequest("test-index");
        request.mapping("{\"_doc\":{\"properties\":{\"field1\":{\"type\":\"keyword\"}}}}");

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        action.innerToXContent(request, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        final String result = BytesReference.bytes(builder).utf8ToString();
        assertTrue(result.contains("mappings"));
        assertTrue(result.contains("properties"));
        assertTrue(result.contains("field1"));
        // The _doc wrapper should be unwrapped
        assertFalse(result.contains("_doc"));
    }
}
