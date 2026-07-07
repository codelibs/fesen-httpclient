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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpResolveIndexActionTest {

    private final HttpResolveIndexAction action = new HttpResolveIndexAction(null, ResolveIndexAction.INSTANCE);

    @Test
    void test_fromXContent() throws IOException {
        final String json = "{" //
                + "\"indices\":[{\"name\":\"idx1\",\"aliases\":[\"a1\"],\"attributes\":[\"open\"],\"data_stream\":\"ds1\"}]," //
                + "\"aliases\":[{\"name\":\"a1\",\"indices\":[\"idx1\"]}]," //
                + "\"data_streams\":[{\"name\":\"ds1\",\"backing_indices\":[\"idx1\"],\"timestamp_field\":\"@timestamp\"}]" //
                + "}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final ResolveIndexAction.Response response = action.fromXContent(parser);
            assertNotNull(response);

            assertEquals(1, response.getIndices().size());
            final ResolvedIndex index = response.getIndices().get(0);
            assertEquals("idx1", index.getName());
            assertArrayEquals(new String[] { "a1" }, index.getAliases());
            assertArrayEquals(new String[] { "open" }, index.getAttributes());
            assertEquals("ds1", index.getDataStream());

            assertEquals(1, response.getAliases().size());
            final ResolvedAlias alias = response.getAliases().get(0);
            assertEquals("a1", alias.getName());
            assertArrayEquals(new String[] { "idx1" }, alias.getIndices());

            assertEquals(1, response.getDataStreams().size());
            final ResolvedDataStream dataStream = response.getDataStreams().get(0);
            assertEquals("ds1", dataStream.getName());
            assertArrayEquals(new String[] { "idx1" }, dataStream.getBackingIndices());
            assertEquals("@timestamp", dataStream.getTimestampField());
        }
    }

    @Test
    void test_fromXContent_empty() throws IOException {
        final String json = "{\"indices\":[],\"aliases\":[],\"data_streams\":[]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final ResolveIndexAction.Response response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(0, response.getIndices().size());
            assertEquals(0, response.getAliases().size());
            assertEquals(0, response.getDataStreams().size());
        }
    }

    @Test
    void test_fromXContent_indexWithoutOptionalFields() throws IOException {
        // ResolvedIndex.toXContent omits "aliases" and "data_stream" when they are empty/null.
        final String json = "{\"indices\":[{\"name\":\"idx1\",\"attributes\":[\"open\",\"hidden\"]}],\"aliases\":[],\"data_streams\":[]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final ResolveIndexAction.Response response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(1, response.getIndices().size());
            final ResolvedIndex index = response.getIndices().get(0);
            assertEquals("idx1", index.getName());
            assertArrayEquals(new String[0], index.getAliases());
            assertArrayEquals(new String[] { "open", "hidden" }, index.getAttributes());
            assertEquals(null, index.getDataStream());
        }
    }
}
