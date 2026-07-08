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

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.support.IndicesOptions;

class HttpForceMergeActionTest {

    private final HttpForceMergeAction action = new HttpForceMergeAction(ActionTestUtils.testClient(), ForceMergeAction.INSTANCE);

    @Test
    void test_getCurlRequest_primaryOnly() {
        final ForceMergeRequest request = new ForceMergeRequest("test-index").primaryOnly(true);
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(request));
        assertEquals("true", params.get("primary_only"));
    }

    @Test
    void test_getCurlRequest_primaryOnly_openSearch2() {
        final HttpForceMergeAction os2Action =
                new HttpForceMergeAction(ActionTestUtils.testClient(EngineType.OPENSEARCH2), ForceMergeAction.INSTANCE);
        final ForceMergeRequest request = new ForceMergeRequest("test-index").primaryOnly(true);
        final Map<String, String> params = ActionTestUtils.params(os2Action.getCurlRequest(request));
        assertEquals("true", params.get("primary_only"));
    }

    @Test
    void test_getCurlRequest_primaryOnly_notSentOnLegacyEngines() {
        // primary_only is unknown to OpenSearch 1.x and Elasticsearch 7/8, which reject it with HTTP 400.
        for (final EngineType engineType : new EngineType[] { EngineType.OPENSEARCH1, EngineType.ELASTICSEARCH7,
                EngineType.ELASTICSEARCH8 }) {
            final HttpForceMergeAction legacyAction =
                    new HttpForceMergeAction(ActionTestUtils.testClient(engineType), ForceMergeAction.INSTANCE);
            final ForceMergeRequest request = new ForceMergeRequest("test-index").primaryOnly(true);
            final Map<String, String> params = ActionTestUtils.params(legacyAction.getCurlRequest(request));
            assertFalse(params.containsKey("primary_only"), engineType.toString());
            // non-version-specific params are still forwarded
            assertEquals(String.valueOf(request.maxNumSegments()), params.get("max_num_segments"), engineType.toString());
        }
    }

    @Test
    void test_getCurlRequest_defaultParams() {
        final ForceMergeRequest request = new ForceMergeRequest("test-index");
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(request));
        assertEquals(String.valueOf(request.primaryOnly()), params.get("primary_only"));
        assertEquals(String.valueOf(request.maxNumSegments()), params.get("max_num_segments"));
        assertEquals(String.valueOf(request.onlyExpungeDeletes()), params.get("only_expunge_deletes"));
        assertEquals(String.valueOf(request.flush()), params.get("flush"));
    }

    @Test
    void test_getCurlRequest_indicesOptions() {
        final ForceMergeRequest request = new ForceMergeRequest("test-index");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, false, true));
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(request));
        assertEquals("true", params.get("ignore_unavailable"));
        assertEquals("false", params.get("allow_no_indices"));
        assertEquals("closed", params.get("expand_wildcards"));
    }
}
