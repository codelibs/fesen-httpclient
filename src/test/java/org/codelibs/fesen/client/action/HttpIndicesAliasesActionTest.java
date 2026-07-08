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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;

class HttpIndicesAliasesActionTest {

    /**
     * Builds the request body exactly as the production action does, by driving the
     * real {@link HttpIndicesAliasesAction#getCurlRequest} and reading the emitted body.
     */
    private static String toBody(final IndicesAliasesRequest request) {
        final HttpIndicesAliasesAction action = new HttpIndicesAliasesAction(ActionTestUtils.testClient(), IndicesAliasesAction.INSTANCE);
        return action.getCurlRequest(request).body();
    }

    @Test
    void test_body_addAction_writeIndexAndHidden() {
        final IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().index("test-index").alias("test-alias").writeIndex(true).isHidden(true));
        final String body = toBody(request);
        assertTrue(body.contains("\"is_write_index\":true"), body);
        assertTrue(body.contains("\"is_hidden\":true"), body);
    }

    @Test
    void test_body_addAction_routing() {
        final IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().index("test-index").alias("test-alias").routing("r1"));
        final String body = toBody(request);
        assertTrue(body.contains("\"routing\":\"r1\""), body);
    }

    @Test
    void test_body_removeIndex_noBogusAliases() {
        final IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.removeIndex().index("old-index"));
        final String body = toBody(request);
        assertTrue(body.contains("remove_index"), body);
        assertFalse(body.contains("\"aliases\""), body);
    }
}
