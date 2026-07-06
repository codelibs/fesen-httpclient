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

import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.UpdateByQueryAction;

class HttpUpdateByQueryActionTest {

    private final HttpUpdateByQueryAction action = new HttpUpdateByQueryAction(null, UpdateByQueryAction.INSTANCE);

    @Test
    void test_fromXContent() throws Exception {
        final String json = "{\"took\":320,\"timed_out\":false,\"total\":100,\"updated\":80,\"created\":0,\"deleted\":0,"
                + "\"batches\":2,\"version_conflicts\":3,\"noops\":17,\"retries\":{\"bulk\":1,\"search\":0},"
                + "\"throttled_millis\":0,\"requests_per_second\":-1.0,\"throttled_until_millis\":0,\"failures\":[]}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final BulkByScrollResponse response = action.fromXContent(parser);
            assertNotNull(response);
            assertEquals(320L, response.getTook().millis());
            assertFalse(response.isTimedOut());
            assertEquals(100L, response.getTotal());
            assertEquals(80L, response.getUpdated());
            assertEquals(0L, response.getCreated());
            assertEquals(0L, response.getDeleted());
            assertEquals(2, response.getBatches());
            assertEquals(3L, response.getVersionConflicts());
            assertEquals(17L, response.getNoops());
            assertEquals(1L, response.getBulkRetries());
            assertEquals(0L, response.getSearchRetries());
            assertEquals(0, response.getBulkFailures().size());
        }
    }
}
