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
import org.opensearch.action.explain.ExplainAction;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

class HttpExplainActionTest {

    private final HttpExplainAction clientAction = new HttpExplainAction(ActionTestUtils.testClient(), ExplainAction.INSTANCE);

    @Test
    void test_getCurlRequest_sourceIncludesExcludes() {
        final ExplainRequest request = new ExplainRequest("test-index", "1")
                .fetchSourceContext(new FetchSourceContext(true, new String[] { "field1" }, new String[] { "excluded" }));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("field1", params.get("_source_includes"));
        assertEquals("excluded", params.get("_source_excludes"));
    }

    @Test
    void test_getCurlRequest_sourceDisabled() {
        final ExplainRequest request = new ExplainRequest("test-index", "1").fetchSourceContext(new FetchSourceContext(false));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("false", params.get("_source"));
        assertFalse(params.containsKey("_source_includes"));
    }
}
