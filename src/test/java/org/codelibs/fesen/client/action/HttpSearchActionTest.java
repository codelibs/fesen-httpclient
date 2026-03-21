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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

class HttpSearchActionTest {

    private final HttpSearchAction action = new HttpSearchAction(null, SearchAction.INSTANCE);

    @Test
    void test_getQuerySource_withDefaultSource() {
        final SearchRequest request = new SearchRequest("test-index");
        final String result = action.getQuerySource(request);
        // SearchRequest initializes with a default SearchSourceBuilder, so result is not null
        assertNotNull(result);
        assertEquals("{}", result);
    }

    @Test
    void test_getQuerySource_withMatchAllQuery() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("match_all"));
    }

    @Test
    void test_getQuerySource_withTermQuery() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("field1", "value1")));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("term"));
        assertTrue(result.contains("field1"));
        assertTrue(result.contains("value1"));
    }

    @Test
    void test_getQuerySource_withSizeAndFrom() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().size(10).from(20));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("\"size\":10"));
        assertTrue(result.contains("\"from\":20"));
    }

    @Test
    void test_getQuerySource_withBoolQuery() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", "active"))
                .mustNot(QueryBuilders.termQuery("deleted", true))));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("bool"));
        assertTrue(result.contains("must"));
        assertTrue(result.contains("must_not"));
    }

    @Test
    void test_getQuerySource_withEmptySource() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder());
        final String result = action.getQuerySource(request);
        assertNotNull(result);
    }

    @Test
    void test_getQuerySource_withSourceFiltering() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().fetchSource(new String[] { "field1", "field2" }, new String[] { "excluded" }));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("_source"));
        assertTrue(result.contains("field1"));
    }

    @Test
    void test_getQuerySource_withAggregation() {
        final SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().size(0)
                .aggregation(org.opensearch.search.aggregations.AggregationBuilders.terms("by_status").field("status")));
        final String result = action.getQuerySource(request);
        assertNotNull(result);
        assertTrue(result.contains("aggs") || result.contains("aggregations"));
        assertTrue(result.contains("by_status"));
    }
}
