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

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.open.OpenIndexAction;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.support.ActiveShardCount;

class HttpOpenIndexActionTest {

    private final HttpOpenIndexAction clientAction = new HttpOpenIndexAction(ActionTestUtils.testClient(), OpenIndexAction.INSTANCE);

    @Test
    void test_getCurlRequest_waitForActiveShardsAll() {
        final OpenIndexRequest request = new OpenIndexRequest("test-index").waitForActiveShards(ActiveShardCount.ALL);
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("all", params.get("wait_for_active_shards"));
    }

    @Test
    void test_getCurlRequest_waitForActiveShardsDefaultNotSent() {
        final OpenIndexRequest request = new OpenIndexRequest("test-index");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertFalse(params.containsKey("wait_for_active_shards"));
    }

    @Test
    void test_getCurlRequest_waitForActiveShardsCount() {
        final OpenIndexRequest request = new OpenIndexRequest("test-index").waitForActiveShards(ActiveShardCount.from(2));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("2", params.get("wait_for_active_shards"));
    }
}
