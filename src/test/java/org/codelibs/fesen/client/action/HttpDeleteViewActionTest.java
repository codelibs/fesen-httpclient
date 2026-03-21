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

import org.codelibs.fesen.client.util.UrlUtils;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.view.DeleteViewAction;

class HttpDeleteViewActionTest {

    @Test
    void test_construction_withNullClient() {
        final HttpDeleteViewAction action = new HttpDeleteViewAction(null, DeleteViewAction.INSTANCE);
        assertNotNull(action);
    }

    @Test
    void test_actionFieldIsSet() {
        final HttpDeleteViewAction action = new HttpDeleteViewAction(null, DeleteViewAction.INSTANCE);
        assertNotNull(action.action);
    }

    @Test
    void test_urlPath_simpleViewName() {
        // Verify the URL path pattern used by getCurlRequest: /views/{encoded_name}
        final String viewName = "my-test-view";
        final String path = "/views/" + UrlUtils.encode(viewName);
        assertEquals("/views/my-test-view", path);
    }

    @Test
    void test_urlPath_viewNameWithSpecialCharacters() {
        final String viewName = "view with spaces";
        final String path = "/views/" + UrlUtils.encode(viewName);
        assertEquals("/views/view+with+spaces", path);
    }

    @Test
    void test_urlPath_viewNameWithSlash() {
        final String viewName = "ns/view";
        final String path = "/views/" + UrlUtils.encode(viewName);
        assertEquals("/views/ns%2Fview", path);
    }
}
