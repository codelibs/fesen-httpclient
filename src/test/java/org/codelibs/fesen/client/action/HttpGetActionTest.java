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

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.codelibs.fesen.opensearch.action.get.GetAction;
import org.codelibs.fesen.opensearch.action.get.GetRequest;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;

class HttpGetActionTest {

    private final HttpGetAction clientAction = new HttpGetAction(ActionTestUtils.testClient(), GetAction.INSTANCE);

    @Test
    void test_construction() {
        final HttpGetAction action = new HttpGetAction(null, GetAction.INSTANCE);
        assertNotNull(action);
    }

    @Test
    void test_getCurlRequest_sourceIncludesExcludes() {
        final GetRequest request = new GetRequest("test-index", "1")
                .fetchSourceContext(new FetchSourceContext(true, new String[] { "field1", "field2" }, new String[] { "excluded" }));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("field1,field2", params.get("_source_includes"));
        assertEquals("excluded", params.get("_source_excludes"));
    }

    @Test
    void test_getCurlRequest_sourceDisabled() {
        final GetRequest request = new GetRequest("test-index", "1").fetchSourceContext(new FetchSourceContext(false));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("false", params.get("_source"));
        assertFalse(params.containsKey("_source_includes"));
    }

    @Test
    void test_getCurlRequest_storedFieldsAndPreference() {
        final GetRequest request = new GetRequest("test-index", "1").storedFields("f1", "f2").preference("_local").routing("r1");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("f1,f2", params.get("stored_fields"));
        assertEquals("_local", params.get("preference"));
        assertEquals("r1", params.get("routing"));
    }
}
