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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpGetIngestionStateActionTest {

    private final HttpGetIngestionStateAction action = new HttpGetIngestionStateAction(null, GetIngestionStateAction.INSTANCE);

    private XContentParser createParser(final String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
    }

    @Test
    void test_fromXContent_basicShards() throws IOException {
        final String json = "{\"_shards\": {\"total\": 5, \"successful\": 5, \"failed\": 0}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals(5, response.getTotalShards());
            assertEquals(5, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
            assertEquals(0, response.getShardStates().length);
        }
    }

    @Test
    void test_fromXContent_withFailedShards() throws IOException {
        final String json = "{\"_shards\": {\"total\": 10, \"successful\": 8, \"failed\": 2}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals(10, response.getTotalShards());
            assertEquals(8, response.getSuccessfulShards());
            assertEquals(2, response.getFailedShards());
        }
    }

    @Test
    void test_fromXContent_withIngestionStateParsed() throws IOException {
        final String json = "{\"_shards\": {\"total\": 2, \"successful\": 2, \"failed\": 0}, " + "\"ingestion_state\": {\"my-index\": ["
                + "{\"shard\": 0, \"poller_state\": \"STARTED\", \"error_policy\": \"DROP\", "
                + "\"poller_paused\": false, \"write_block_enabled\": false, "
                + "\"batch_start_pointer\": \"100\", \"is_primary\": true, \"node\": \"node1\"},"
                + "{\"shard\": 1, \"poller_state\": \"PAUSED\", \"error_policy\": \"BLOCK\", "
                + "\"poller_paused\": true, \"write_block_enabled\": true, "
                + "\"batch_start_pointer\": \"200\", \"is_primary\": false, \"node\": \"node2\"}" + "]}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals(2, response.getTotalShards());
            assertEquals(2, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());

            final ShardIngestionState[] states = response.getShardStates();
            assertNotNull(states);
            assertEquals(2, states.length);

            assertEquals("my-index", states[0].getIndex());
            assertEquals(0, states[0].getShardId());
            assertEquals("STARTED", states[0].getPollerState());
            assertEquals("DROP", states[0].getErrorPolicy());
            assertFalse(states[0].isPollerPaused());
            assertFalse(states[0].isWriteBlockEnabled());
            assertEquals("100", states[0].getBatchStartPointer());
            assertTrue(states[0].isPrimary());
            assertEquals("node1", states[0].getNodeName());

            assertEquals("my-index", states[1].getIndex());
            assertEquals(1, states[1].getShardId());
            assertEquals("PAUSED", states[1].getPollerState());
            assertEquals("BLOCK", states[1].getErrorPolicy());
            assertTrue(states[1].isPollerPaused());
            assertTrue(states[1].isWriteBlockEnabled());
            assertEquals("200", states[1].getBatchStartPointer());
            assertFalse(states[1].isPrimary());
            assertEquals("node2", states[1].getNodeName());
        }
    }

    @Test
    void test_fromXContent_withMultipleIndices() throws IOException {
        final String json = "{\"_shards\": {\"total\": 2, \"successful\": 2, \"failed\": 0}, " + "\"ingestion_state\": {"
                + "\"index-a\": [{\"shard\": 0, \"poller_state\": \"STARTED\", \"error_policy\": \"DROP\", "
                + "\"poller_paused\": false, \"write_block_enabled\": false, \"batch_start_pointer\": \"0\", "
                + "\"is_primary\": true, \"node\": \"n1\"}],"
                + "\"index-b\": [{\"shard\": 0, \"poller_state\": \"PAUSED\", \"error_policy\": \"BLOCK\", "
                + "\"poller_paused\": true, \"write_block_enabled\": false, \"batch_start_pointer\": \"50\", "
                + "\"is_primary\": true, \"node\": \"n2\"}]" + "}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            final ShardIngestionState[] states = response.getShardStates();
            assertEquals(2, states.length);

            // States should contain entries from both indices
            assertTrue(states[0].getIndex().equals("index-a") || states[0].getIndex().equals("index-b"));
            assertTrue(states[1].getIndex().equals("index-a") || states[1].getIndex().equals("index-b"));
        }
    }

    @Test
    void test_fromXContent_withNextPageToken() throws IOException {
        final String json = "{\"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0}, " + "\"next_page_token\": \"abc123\", "
                + "\"ingestion_state\": {\"my-index\": [" + "{\"shard\": 0, \"poller_state\": \"STARTED\", \"error_policy\": \"DROP\", "
                + "\"poller_paused\": false, \"write_block_enabled\": false, "
                + "\"batch_start_pointer\": \"0\", \"is_primary\": true, \"node\": \"node1\"}" + "]}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals("abc123", response.getNextPageToken());
            assertEquals(1, response.getShardStates().length);
        }
    }

    @Test
    void test_fromXContent_emptyShards() throws IOException {
        final String json = "{\"_shards\": {}}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals(0, response.getTotalShards());
            assertEquals(0, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
            assertNull(response.getNextPageToken());
        }
    }

    @Test
    void test_fromXContent_emptyResponse() throws IOException {
        final String json = "{}";
        try (XContentParser parser = createParser(json)) {
            final GetIngestionStateResponse response = action.fromXContent(parser);
            assertEquals(0, response.getTotalShards());
            assertEquals(0, response.getShardStates().length);
        }
    }

    @Test
    void test_fromXContent_invalidToken() {
        assertThrows(IOException.class, () -> {
            final String json = "[\"not_an_object\"]";
            try (XContentParser parser = createParser(json)) {
                action.fromXContent(parser);
            }
        });
    }
}
