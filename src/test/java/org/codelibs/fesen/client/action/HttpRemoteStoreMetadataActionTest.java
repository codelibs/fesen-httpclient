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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
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
    void test_fromXContent_withIndicesSkipped() throws IOException {
        final String json = "{\"_shards\": {\"total\": 3, \"successful\": 3, \"failed\": 0}, "
                + "\"indices\": {\"my_index\": {\"shards\": {\"0\": {\"segment\": {}}}}}}";
        try (XContentParser parser = createParser(json)) {
            final RemoteStoreMetadataResponse response = action.fromXContent(parser);
            assertEquals(3, response.getTotalShards());
            assertEquals(3, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
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
