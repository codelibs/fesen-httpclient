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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreShardMetadata;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpRemoteStoreMetadataActionTest {

    private final HttpRemoteStoreMetadataAction action = new HttpRemoteStoreMetadataAction(null, RemoteStoreMetadataAction.INSTANCE);

    private XContentParser createParser(final String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
    }

    @Test
    void test_fromXContent_basicShards() throws IOException {
        final String json = "{\"_shards\": {\"total\": 5, \"successful\": 5, \"failed\": 0}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(5, response.getTotalShards());
            assertEquals(5, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
        }
    }

    @Test
    void test_fromXContent_withFailedShards() throws IOException {
        final String json = "{\"_shards\": {\"total\": 10, \"successful\": 8, \"failed\": 2}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(10, response.getTotalShards());
            assertEquals(8, response.getSuccessfulShards());
            assertEquals(2, response.getFailedShards());
        }
    }

    @Test
    void test_fromXContent_withIndicesParsed() throws IOException {
        final String json =
                "{\"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0}, " + "\"indices\": {\"my_index\": {\"shards\": {\"0\": [{"
                        + "\"index\": \"my_index\", \"shard\": 0, " + "\"latest_segment_metadata_filename\": \"seg_meta_001\", "
                        + "\"latest_translog_metadata_filename\": \"translog_meta_001\", "
                        + "\"available_segment_metadata_files\": {\"file1\": {\"size\": 1024}}, "
                        + "\"available_translog_metadata_files\": {\"tfile1\": {\"size\": 512}}" + "}]}}}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(1, response.getTotalShards());

            final Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> grouped = response.groupByIndexAndShards();
            assertNotNull(grouped);
            assertTrue(grouped.containsKey("my_index"));

            final List<RemoteStoreShardMetadata> shardMetadata = grouped.get("my_index").get(0);
            assertNotNull(shardMetadata);
            assertEquals(1, shardMetadata.size());

            final RemoteStoreShardMetadata metadata = shardMetadata.get(0);
            assertEquals("my_index", metadata.getIndexName());
            assertEquals(0, metadata.getShardId());
            assertEquals("seg_meta_001", metadata.getLatestSegmentMetadataFileName());
            assertEquals("translog_meta_001", metadata.getLatestTranslogMetadataFileName());
            assertNotNull(metadata.getSegmentMetadataFiles());
            assertTrue(metadata.getSegmentMetadataFiles().containsKey("file1"));
            assertNotNull(metadata.getTranslogMetadataFiles());
            assertTrue(metadata.getTranslogMetadataFiles().containsKey("tfile1"));
        }
    }

    @Test
    void test_fromXContent_withMultipleIndicesAndShards() throws IOException {
        final String json = "{\"_shards\": {\"total\": 2, \"successful\": 2, \"failed\": 0}, " + "\"indices\": {"
                + "\"index_a\": {\"shards\": {" + "\"0\": [{\"index\": \"index_a\", \"shard\": 0, "
                + "\"available_segment_metadata_files\": {}, \"available_translog_metadata_files\": {}}]" + "}},"
                + "\"index_b\": {\"shards\": {" + "\"0\": [{\"index\": \"index_b\", \"shard\": 0, "
                + "\"available_segment_metadata_files\": {}, \"available_translog_metadata_files\": {}}]" + "}}" + "}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            final Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> grouped = response.groupByIndexAndShards();
            assertEquals(2, grouped.size());
            assertTrue(grouped.containsKey("index_a"));
            assertTrue(grouped.containsKey("index_b"));
        }
    }

    @Test
    void test_fromXContent_withoutMetadataFileNames() throws IOException {
        final String json = "{\"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0}, "
                + "\"indices\": {\"my_index\": {\"shards\": {\"0\": [{" + "\"index\": \"my_index\", \"shard\": 0, "
                + "\"available_segment_metadata_files\": {}, " + "\"available_translog_metadata_files\": {}" + "}]}}}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            final Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> grouped = response.groupByIndexAndShards();
            final RemoteStoreShardMetadata metadata = grouped.get("my_index").get(0).get(0);
            assertNull(metadata.getLatestSegmentMetadataFileName());
            assertNull(metadata.getLatestTranslogMetadataFileName());
        }
    }

    @Test
    void test_fromXContent_emptyShards() throws IOException {
        final String json = "{\"_shards\": {}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(0, response.getTotalShards());
            assertEquals(0, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
        }
    }

    @Test
    void test_fromXContent_emptyResponse() throws IOException {
        final String json = "{}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(0, response.getTotalShards());
            assertEquals(0, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
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
