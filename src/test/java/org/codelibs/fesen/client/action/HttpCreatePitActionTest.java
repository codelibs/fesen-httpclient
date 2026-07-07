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
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpCreatePitActionTest {

    private final HttpCreatePitAction action = new HttpCreatePitAction(null, CreatePitAction.INSTANCE);

    @Test
    void test_fromXContent() throws IOException {
        final String json = "{\"pit_id\":\"abc\",\"_shards\":{\"total\":2,\"successful\":2,\"skipped\":0,\"failed\":0},"
                + "\"creation_time\":1720000000000}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final CreatePitResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals("abc", response.getId());
            assertEquals(1720000000000L, response.getCreationTime());
            assertEquals(2, response.getTotalShards());
            assertEquals(2, response.getSuccessfulShards());
            assertEquals(0, response.getSkippedShards());
            assertEquals(0, response.getFailedShards());
        }
    }
}
