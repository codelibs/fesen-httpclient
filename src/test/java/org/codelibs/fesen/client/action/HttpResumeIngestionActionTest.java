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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionAction;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpResumeIngestionActionTest {

    private final HttpResumeIngestionAction action = new HttpResumeIngestionAction(null, ResumeIngestionAction.INSTANCE);

    private XContentParser createParser(final String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
    }

    @Test
    void test_fromXContent_allTrue() throws IOException {
        final String json = "{\"acknowledged\": true, \"shards_acknowledged\": true}";
        try (XContentParser parser = createParser(json)) {
            final ResumeIngestionResponse response = action.fromXContent(parser);
            assertTrue(response.isAcknowledged());
            assertTrue(response.isShardsAcknowledged());
        }
    }

    @Test
    void test_fromXContent_acknowledgedTrueShardsAcknowledgedFalse() throws IOException {
        final String json = "{\"acknowledged\": true, \"shards_acknowledged\": false}";
        try (XContentParser parser = createParser(json)) {
            final ResumeIngestionResponse response = action.fromXContent(parser);
            assertTrue(response.isAcknowledged());
            assertFalse(response.isShardsAcknowledged());
        }
    }

    @Test
    void test_fromXContent_allFalse() throws IOException {
        final String json = "{\"acknowledged\": false, \"shards_acknowledged\": false}";
        try (XContentParser parser = createParser(json)) {
            final ResumeIngestionResponse response = action.fromXContent(parser);
            assertFalse(response.isAcknowledged());
            assertFalse(response.isShardsAcknowledged());
        }
    }

    @Test
    void test_fromXContent_withExtraFields() throws IOException {
        final String json = "{\"acknowledged\": true, \"shards_acknowledged\": true, "
                + "\"error\": {\"type\": \"some_error\", \"reason\": \"test\"}, " + "\"failures\": [{\"shard\": 0}]}";
        try (XContentParser parser = createParser(json)) {
            final ResumeIngestionResponse response = action.fromXContent(parser);
            assertTrue(response.isAcknowledged());
            assertTrue(response.isShardsAcknowledged());
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
