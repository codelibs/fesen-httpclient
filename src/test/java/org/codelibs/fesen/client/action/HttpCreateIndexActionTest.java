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
import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

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

    /**
     * An alias the caller did not mark must not carry is_write_index at all. Sending false makes
     * the alias read-only, so indexing through it fails with "no write index is defined for
     * alias"; sending null is rejected outright by Elasticsearch 8 with
     * "Unknown token [VALUE_NULL] in alias". Omitting the field is the only rendering both
     * engines read as "the caller did not decide".
     */
    @Test
    void test_innerToXContent_aliasWriteIndexOmittedWhenCallerDidNotSetIt() throws IOException {
        final String rendered = renderAliases(new Alias("test-alias"));
        assertFalse(rendered.contains("is_write_index"));
        assertTrue(rendered.contains("\"test-alias\""));
    }

    @Test
    void test_innerToXContent_aliasWriteIndexKeptWhenCallerSetIt() throws IOException {
        assertTrue(renderAliases(new Alias("test-alias").writeIndex(true)).contains("\"is_write_index\":true"));
        assertTrue(renderAliases(new Alias("test-alias").writeIndex(false)).contains("\"is_write_index\":false"));
    }

    @Test
    void test_innerToXContent_aliasOptionalFieldsOmittedUnlessSet() throws IOException {
        assertFalse(renderAliases(new Alias("test-alias")).contains("is_hidden"));
        assertFalse(renderAliases(new Alias("test-alias")).contains("routing"));
        assertTrue(renderAliases(new Alias("test-alias").isHidden(true)).contains("\"is_hidden\":true"));
        assertTrue(renderAliases(new Alias("test-alias").routing("r")).contains("\"routing\":\"r\""));
        final String split = renderAliases(new Alias("test-alias").indexRouting("i").searchRouting("s"));
        assertTrue(split.contains("\"index_routing\":\"i\""));
        assertTrue(split.contains("\"search_routing\":\"s\""));
    }

    private String renderAliases(final Alias alias) throws IOException {
        final HttpCreateIndexAction action = new HttpCreateIndexAction(null, CreateIndexAction.INSTANCE);
        final CreateIndexRequest request = new CreateIndexRequest("test-index").alias(alias);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        action.innerToXContent(request, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return BytesReference.bytes(builder).utf8ToString();
    }
}
