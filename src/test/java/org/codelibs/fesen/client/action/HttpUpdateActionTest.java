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
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateRequest;

class HttpUpdateActionTest {

    private final HttpUpdateAction clientAction = new HttpUpdateAction(ActionTestUtils.testClient(), UpdateAction.INSTANCE);

    /**
     * The {@code _update} REST endpoint (RestUpdateAction) rejects {@code version}/{@code version_type}
     * with HTTP 400 because internal versioning is illegal for updates (UpdateRequest itself throws
     * UnsupportedOperationException on its version/versionType setters and always reports MATCH_ANY /
     * INTERNAL). Concurrency control must go through if_seq_no/if_primary_term instead. This action must
     * therefore never emit version params.
     */
    @Test
    void test_getCurlRequest_versionParamsNotSent() {
        final UpdateRequest request = new UpdateRequest("test-index", "1");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertFalse(params.containsKey("version"));
        assertFalse(params.containsKey("version_type"));
    }

    @Test
    void test_getCurlRequest_ifSeqNoAndPrimaryTerm() {
        final UpdateRequest request = new UpdateRequest("test-index", "1").setIfSeqNo(5L).setIfPrimaryTerm(2L);
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("5", params.get("if_seq_no"));
        assertEquals("2", params.get("if_primary_term"));
    }
}
