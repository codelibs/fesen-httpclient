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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaType;

class HttpActionTest {

    private final HttpAction action = new HttpAction(null);

    @Test
    void test_fromMediaTypeOrFormat_json() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("application/json");
        assertEquals(XContentType.JSON, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_jsonWithCharset() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("application/json; charset=UTF-8");
        assertEquals(XContentType.JSON, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_yaml() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("application/yaml");
        assertEquals(XContentType.YAML, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_cbor() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("application/cbor");
        assertEquals(XContentType.CBOR, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_smile() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("application/smile");
        assertEquals(XContentType.SMILE, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_null_defaultsToJson() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat(null);
        assertEquals(XContentType.JSON, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_unknown_defaultsToJson() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("text/plain");
        assertEquals(XContentType.JSON, result);
    }

    @Test
    void test_fromMediaTypeOrFormat_subtypeOnly() {
        final MediaType result = HttpAction.fromMediaTypeOrFormat("json");
        assertEquals(XContentType.JSON, result);
    }

    @Test
    void test_getActiveShardsCountValue_all() throws IOException {
        final int value = action.getActiveShardsCountValue(ActiveShardCount.ALL);
        assertEquals(-1, value);
    }

    @Test
    void test_getActiveShardsCountValue_default() throws IOException {
        final int value = action.getActiveShardsCountValue(ActiveShardCount.DEFAULT);
        assertEquals(-2, value);
    }

    @Test
    void test_getActiveShardsCountValue_one() throws IOException {
        final int value = action.getActiveShardsCountValue(ActiveShardCount.ONE);
        assertEquals(1, value);
    }

    @Test
    void test_getActiveShardsCountValue_none() throws IOException {
        final int value = action.getActiveShardsCountValue(ActiveShardCount.NONE);
        assertEquals(0, value);
    }

    @Test
    void test_getActiveShardsCountValue_customValue() throws IOException {
        final int value = action.getActiveShardsCountValue(ActiveShardCount.from(3));
        assertEquals(3, value);
    }

    @Test
    void test_unwrapOpenSearchException_withOpenSearchCause() {
        final OpenSearchException cause = new OpenSearchException("inner error");
        final Exception wrapper = new RuntimeException("outer", cause);
        final boolean[] called = { false };
        action.unwrapOpenSearchException(new org.opensearch.core.action.ActionListener<>() {
            @Override
            public void onResponse(final Object o) {
            }

            @Override
            public void onFailure(final Exception e) {
                called[0] = true;
                assertEquals(cause, e);
            }
        }, wrapper);
        assertEquals(true, called[0]);
    }

    @Test
    void test_unwrapOpenSearchException_withoutOpenSearchCause() {
        final RuntimeException original = new RuntimeException("direct error");
        final boolean[] called = { false };
        action.unwrapOpenSearchException(new org.opensearch.core.action.ActionListener<>() {
            @Override
            public void onResponse(final Object o) {
            }

            @Override
            public void onFailure(final Exception e) {
                called[0] = true;
                assertEquals(original, e);
            }
        }, original);
        assertEquals(true, called[0]);
    }

    @Test
    void test_parseFields_areNotNull() {
        assertNotNull(HttpAction.SHARD_FIELD);
        assertNotNull(HttpAction.INDEX_FIELD);
        assertNotNull(HttpAction.QUERY_FIELD);
        assertNotNull(HttpAction.REASON_FIELD);
        assertNotNull(HttpAction.ALIASES_FIELD);
        assertNotNull(HttpAction.MAPPINGS_FIELD);
        assertNotNull(HttpAction.TYPE_FIELD);
        assertNotNull(HttpAction.DETAILS_FIELD);
        assertNotNull(HttpAction._SHARDS_FIELD);
        assertNotNull(HttpAction.TASKS_FIELD);
        assertNotNull(HttpAction.TOTAL_FIELD);
        assertNotNull(HttpAction.SUCCESSFUL_FIELD);
        assertNotNull(HttpAction.FAILED_FIELD);
        assertNotNull(HttpAction.FAILURES_FIELD);
    }
}
