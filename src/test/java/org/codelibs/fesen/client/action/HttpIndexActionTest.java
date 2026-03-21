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

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpIndexActionTest {

    private final HttpIndexAction action = new HttpIndexAction(null, IndexAction.INSTANCE);

    @Test
    void test_fromXContent_created() throws IOException {
        final String json = "{\"_index\":\"test\",\"_id\":\"1\",\"_version\":1,\"result\":\"created\","
                + "\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}," + "\"_seq_no\":0,\"_primary_term\":1}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final IndexResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals("test", response.getIndex());
            assertEquals("1", response.getId());
            assertEquals(1, response.getVersion());
            assertEquals("created", response.getResult().getLowercase());
        }
    }

    @Test
    void test_fromXContent_updated() throws IOException {
        final String json = "{\"_index\":\"test\",\"_id\":\"1\",\"_version\":2,\"result\":\"updated\","
                + "\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}," + "\"_seq_no\":1,\"_primary_term\":1}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final IndexResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(2, response.getVersion());
            assertEquals("updated", response.getResult().getLowercase());
        }
    }
}
