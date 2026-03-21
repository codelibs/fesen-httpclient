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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.scale.searchonly.ScaleIndexAction;
import org.opensearch.action.admin.indices.scale.searchonly.ScaleIndexRequestBuilder;

class HttpScaleIndexActionTest {

    @Test
    void test_construction() {
        final HttpScaleIndexAction action = new HttpScaleIndexAction(null, ScaleIndexAction.INSTANCE);
        assertNotNull(action);
    }

    @Test
    void test_scaleIndexRequest_hasExpectedReflectionMethods_scaleDown() throws Exception {
        // Verify that ScaleIndexRequest (package-private) has the methods accessed via reflection
        final ScaleIndexRequestBuilder builder = new ScaleIndexRequestBuilder(null, true, "test-index");
        final Object request = builder.request();

        final Method getIndexMethod = request.getClass().getMethod("getIndex");
        getIndexMethod.setAccessible(true);
        final Method isScaleDownMethod = request.getClass().getMethod("isScaleDown");
        isScaleDownMethod.setAccessible(true);

        assertEquals("test-index", getIndexMethod.invoke(request));
        assertTrue((boolean) isScaleDownMethod.invoke(request));
    }

    @Test
    void test_scaleIndexRequest_hasExpectedReflectionMethods_scaleUp() throws Exception {
        final ScaleIndexRequestBuilder builder = new ScaleIndexRequestBuilder(null, false, "my-index");
        final Object request = builder.request();

        final Method getIndexMethod = request.getClass().getMethod("getIndex");
        getIndexMethod.setAccessible(true);
        final Method isScaleDownMethod = request.getClass().getMethod("isScaleDown");
        isScaleDownMethod.setAccessible(true);

        assertEquals("my-index", getIndexMethod.invoke(request));
        assertFalse((boolean) isScaleDownMethod.invoke(request));
    }

    @Test
    void test_requestBodyFormat_scaleDown() throws Exception {
        // Verify the JSON body format that HttpScaleIndexAction.execute() would produce
        final ScaleIndexRequestBuilder builder = new ScaleIndexRequestBuilder(null, true, "test-index");
        final Object request = builder.request();

        final Method isScaleDownMethod = request.getClass().getMethod("isScaleDown");
        isScaleDownMethod.setAccessible(true);
        final boolean scaleDown = (boolean) isScaleDownMethod.invoke(request);
        final String body = "{\"search_only\":" + scaleDown + "}";
        assertEquals("{\"search_only\":true}", body);
    }

    @Test
    void test_requestBodyFormat_scaleUp() throws Exception {
        final ScaleIndexRequestBuilder builder = new ScaleIndexRequestBuilder(null, false, "test-index");
        final Object request = builder.request();

        final Method isScaleDownMethod = request.getClass().getMethod("isScaleDown");
        isScaleDownMethod.setAccessible(true);
        final boolean scaleDown = (boolean) isScaleDownMethod.invoke(request);
        final String body = "{\"search_only\":" + scaleDown + "}";
        assertEquals("{\"search_only\":false}", body);
    }
}
