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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.view.SearchViewAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

class HttpSearchViewActionTest {

    private final HttpSearchViewAction clientAction = new HttpSearchViewAction(ActionTestUtils.testClient(), SearchViewAction.INSTANCE);

    @Test
    void test_getCurlRequest_routingPreferenceAndCcsOmittedWithPit() {
        // RestSearchAction#preparePointInTime rejects [routing], [preference] and
        // [ccs_minimize_roundtrips] together with a point in time.
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", new SearchRequest());
        request.routing("r1");
        request.preference("_local");
        request.source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("pit-id")));
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertFalse(params.containsKey("routing"));
        assertFalse(params.containsKey("preference"));
        assertFalse(params.containsKey("ccs_minimize_roundtrips"));
    }

    @Test
    void test_getCurlRequest_routingPreferenceAndCcsKeptWithoutPit() {
        // Without a PIT all three must still be sent.
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", new SearchRequest());
        request.routing("r1");
        request.preference("_local");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertEquals("r1", params.get("routing"));
        assertEquals("_local", params.get("preference"));
        assertEquals("true", params.get("ccs_minimize_roundtrips"));
    }

    @Test
    void test_construction_withNullClient() {
        final HttpSearchViewAction action = new HttpSearchViewAction(null, SearchViewAction.INSTANCE);
        assertNotNull(action);
    }

    @Test
    void test_getQuerySource_withNullSource() {
        final HttpSearchViewAction action = new HttpSearchViewAction(null, SearchViewAction.INSTANCE);
        final SearchRequest searchRequest = new SearchRequest();
        // Do not set source - it should be null
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", searchRequest);

        final String querySource = action.getQuerySource(request);
        assertNull(querySource);
    }

    @Test
    void test_getQuerySource_withMatchAllQuery() {
        final HttpSearchViewAction action = new HttpSearchViewAction(null, SearchViewAction.INSTANCE);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", searchRequest);

        final String querySource = action.getQuerySource(request);
        assertNotNull(querySource);
        assertTrue(querySource.contains("match_all"));
    }

    @Test
    void test_getQuerySource_withTermQuery() {
        final HttpSearchViewAction action = new HttpSearchViewAction(null, SearchViewAction.INSTANCE);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("status", "active")));
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", searchRequest);

        final String querySource = action.getQuerySource(request);
        assertNotNull(querySource);
        assertTrue(querySource.contains("term"));
        assertTrue(querySource.contains("status"));
        assertTrue(querySource.contains("active"));
    }

    @Test
    void test_getQuerySource_withSizeAndFrom() {
        final HttpSearchViewAction action = new HttpSearchViewAction(null, SearchViewAction.INSTANCE);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().size(10).from(20));
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", searchRequest);

        final String querySource = action.getQuerySource(request);
        assertNotNull(querySource);
        assertTrue(querySource.contains("\"size\":10"));
        assertTrue(querySource.contains("\"from\":20"));
    }
}
