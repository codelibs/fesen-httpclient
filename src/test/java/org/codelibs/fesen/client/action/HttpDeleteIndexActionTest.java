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

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.IndicesOptions;

class HttpDeleteIndexActionTest {

    private final HttpDeleteIndexAction action = new HttpDeleteIndexAction(ActionTestUtils.testClient(), DeleteIndexAction.INSTANCE);

    @Test
    void test_getCurlRequest_indicesOptions() {
        final DeleteIndexRequest request = new DeleteIndexRequest("test-index");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, false, true));
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(request));
        assertEquals("true", params.get("ignore_unavailable"));
        assertEquals("false", params.get("allow_no_indices"));
        assertEquals("closed", params.get("expand_wildcards"));
    }
}
