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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpUpgradeStatusActionTest {

    private final HttpUpgradeStatusAction action = new HttpUpgradeStatusAction(null, UpgradeStatusAction.INSTANCE);

    @Test
    void test_fromXContent() throws IOException {
        // Exact aggregate-only shape returned by the _upgrade status API (verified on OpenSearch 3.7).
        final String json = "{" //
                + "\"size_in_bytes\":4243," //
                + "\"size_to_upgrade_in_bytes\":0," //
                + "\"size_to_upgrade_ancient_in_bytes\":0," //
                + "\"indices\":{" //
                + "\"idx\":{\"size_in_bytes\":4243,\"size_to_upgrade_in_bytes\":0,\"size_to_upgrade_ancient_in_bytes\":0}" //
                + "}}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final UpgradeStatusResponse resp = action.fromXContent(parser);
            assertNotNull(resp);
            // Byte totals are re-derived from the synthetic per-index shards.
            assertEquals(4243L, resp.getTotalBytes());
            assertEquals(0L, resp.getToUpgradeBytes());
            assertEquals(0L, resp.getToUpgradeBytesAncient());
            assertEquals(1, resp.getIndices().size());
            assertNotNull(resp.getIndices().get("idx"));
            assertEquals(4243L, resp.getIndices().get("idx").getTotalBytes());
        }
    }

    @Test
    void test_fromXContent_multipleIndices() throws IOException {
        final String json = "{" //
                + "\"size_in_bytes\":300," //
                + "\"size_to_upgrade_in_bytes\":30," //
                + "\"size_to_upgrade_ancient_in_bytes\":5," //
                + "\"indices\":{" //
                + "\"a\":{\"size_in_bytes\":100,\"size_to_upgrade_in_bytes\":10,\"size_to_upgrade_ancient_in_bytes\":5}," //
                + "\"b\":{\"size_in_bytes\":200,\"size_to_upgrade_in_bytes\":20,\"size_to_upgrade_ancient_in_bytes\":0}" //
                + "}}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final UpgradeStatusResponse resp = action.fromXContent(parser);
            assertNotNull(resp);
            assertEquals(2, resp.getIndices().size());
            // Aggregate totals sum the per-index synthetic shards.
            assertEquals(300L, resp.getTotalBytes());
            assertEquals(30L, resp.getToUpgradeBytes());
            assertEquals(5L, resp.getToUpgradeBytesAncient());
            assertEquals(100L, resp.getIndices().get("a").getTotalBytes());
            assertEquals(10L, resp.getIndices().get("a").getToUpgradeBytes());
            assertEquals(5L, resp.getIndices().get("a").getToUpgradeBytesAncient());
            assertEquals(200L, resp.getIndices().get("b").getTotalBytes());
            assertEquals(20L, resp.getIndices().get("b").getToUpgradeBytes());
        }
    }

    @Test
    void test_fromXContent_emptyIndices() throws IOException {
        // A missing/empty "indices" object must yield an all-empty response rather than fail.
        final String json = "{\"size_in_bytes\":0,\"size_to_upgrade_in_bytes\":0,\"size_to_upgrade_ancient_in_bytes\":0,\"indices\":{}}";
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            final UpgradeStatusResponse resp = action.fromXContent(parser);
            assertNotNull(resp);
            assertTrue(resp.getIndices().isEmpty());
            assertEquals(0L, resp.getTotalBytes());
        }
    }
}
