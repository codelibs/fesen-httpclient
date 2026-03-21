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

import org.junit.jupiter.api.Test;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpBulkActionTest {

    private final HttpBulkAction action = new HttpBulkAction(null, BulkAction.INSTANCE);

    @Test
    void test_getStringfromDocWriteRequest_indexWithId() {
        final IndexRequest request = new IndexRequest("test-index").id("doc1").source("{\"field\":\"value\"}", XContentType.JSON);
        final String result = action.getStringfromDocWriteRequest(request);
        assertTrue(result.contains("\"index\""));
        assertTrue(result.contains("\"_index\":\"test-index\""));
        assertTrue(result.contains("\"_id\":\"doc1\""));
    }

    @Test
    void test_getStringfromDocWriteRequest_indexWithoutId() {
        final IndexRequest request = new IndexRequest("test-index").source("{\"field\":\"value\"}", XContentType.JSON);
        request.id(null);
        final String result = action.getStringfromDocWriteRequest(request);
        assertTrue(result.contains("\"index\"") || result.contains("\"create\""));
        assertTrue(result.contains("\"_index\":\"test-index\""));
        assertFalse(result.contains("\"_id\""));
    }

    @Test
    void test_getStringfromDocWriteRequest_delete() {
        final DeleteRequest request = new DeleteRequest("test-index", "doc1");
        final String result = action.getStringfromDocWriteRequest(request);
        assertTrue(result.contains("\"delete\""));
        assertTrue(result.contains("\"_index\":\"test-index\""));
        assertTrue(result.contains("\"_id\":\"doc1\""));
    }

    @Test
    void test_getStringfromDocWriteRequest_withRouting() {
        final IndexRequest request = new IndexRequest("test-index").id("doc1").routing("r1")
                .source("{\"field\":\"value\"}", XContentType.JSON);
        final String result = action.getStringfromDocWriteRequest(request);
        assertTrue(result.contains("\"routing\":\"r1\""));
    }

    @Test
    void test_getStringfromDocWriteRequest_withPipeline() {
        final IndexRequest request =
                new IndexRequest("test-index").id("doc1").setPipeline("my-pipeline").source("{\"field\":\"value\"}", XContentType.JSON);
        final String result = action.getStringfromDocWriteRequest(request);
        assertTrue(result.contains("\"pipeline\":\"my-pipeline\""));
    }

    @Test
    void test_fromXContent_emptyBulkResponse() throws IOException {
        final String json = "{\"took\":10,\"errors\":false,\"items\":[]}";
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(10, response.getTook().millis());
            assertFalse(response.hasFailures());
            assertEquals(0, response.getItems().length);
        }
    }

    @Test
    void test_fromXContent_withIngestTook() throws IOException {
        final String json = "{\"took\":5,\"ingest_took\":3,\"errors\":false,\"items\":[]}";
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(5, response.getTook().millis());
            assertEquals(3, response.getIngestTookInMillis());
        }
    }

    @Test
    void test_fromXContent_withIndexItem() throws IOException {
        final String json = "{\"took\":1,\"errors\":false,\"items\":[{\"index\":{\"_index\":\"test\",\"_id\":\"1\","
                + "\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0},"
                + "\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}";
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getItems().length);
            assertEquals("test", response.getItems()[0].getIndex());
            assertEquals("1", response.getItems()[0].getId());
            assertFalse(response.getItems()[0].isFailed());
        }
    }

    @Test
    void test_fromXContent_withDeleteItem() throws IOException {
        final String json = "{\"took\":1,\"errors\":false,\"items\":[{\"delete\":{\"_index\":\"test\",\"_id\":\"1\","
                + "\"_version\":2,\"result\":\"deleted\",\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0},"
                + "\"_seq_no\":1,\"_primary_term\":1,\"status\":200}}]}";
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getItems().length);
            assertFalse(response.getItems()[0].isFailed());
        }
    }

    @Test
    void test_fromXContent_withFailedItem() throws IOException {
        final String json = "{\"took\":1,\"errors\":true,\"items\":[{\"index\":{\"_index\":\"test\",\"_id\":\"1\","
                + "\"status\":400,\"error\":{\"type\":\"mapper_parsing_exception\","
                + "\"reason\":\"failed to parse\"}}}]}";
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertTrue(response.hasFailures());
            assertEquals(1, response.getItems().length);
            assertTrue(response.getItems()[0].isFailed());
        }
    }
}
