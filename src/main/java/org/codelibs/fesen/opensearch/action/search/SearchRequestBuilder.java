/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.Scroll;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.codelibs.fesen.opensearch.search.builder.PointInTimeBuilder;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.opensearch.search.collapse.CollapseBuilder;
import org.codelibs.fesen.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.codelibs.fesen.opensearch.search.rescore.RescorerBuilder;
import org.codelibs.fesen.opensearch.search.slice.SliceBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortOrder;
import org.codelibs.fesen.opensearch.search.suggest.SuggestBuilder;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Arrays;
import java.util.List;

/**
 * A search action request builder.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchRequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse> {

    public SearchRequestBuilder(OpenSearchClient client, SearchAction action) {
        super(client, action, new SearchRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequestBuilder setSearchType(String searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchRequestBuilder setTimeout(TimeValue timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional document count, upon collecting which the search
     * query will early terminate
     */
    public SearchRequestBuilder setTerminateAfter(int terminateAfter) {
        sourceBuilder().terminateAfter(terminateAfter);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public SearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.codelibs.fesen.opensearch.index.query.QueryBuilders
     */
    public SearchRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public SearchRequestBuilder setMinScore(float minScore) {
        sourceBuilder().minScore(minScore);
        return this;
    }

    /**
     * From index to start the search from. Defaults to {@code 0}.
     */
    public SearchRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to {@code 10}.
     */
    public SearchRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public SearchRequestBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned with its
     * version.
     */
    public SearchRequestBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned with the
     * sequence number and primary term of the last modification of the document.
     */
    public SearchRequestBuilder seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        sourceBuilder().seqNoAndPrimaryTerm(seqNoAndPrimaryTerm);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public SearchRequestBuilder setFetchSource(boolean fetch) {
        sourceBuilder().fetchSource(fetch);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public SearchRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public SearchRequestBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.codelibs.fesen.opensearch.search.sort.SortBuilders
     */
    public SearchRequestBuilder addSort(SortBuilder<?> sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Set the sort values that indicates which docs this request should "search after".
     *
     */
    public SearchRequestBuilder searchAfter(Object[] values) {
        sourceBuilder().searchAfter(values);
        return this;
    }

    /**
     * Indicates if the total hit count for the query should be tracked. Requests will count total hit count accurately
     * up to 10,000 by default, see {@link #setTrackTotalHitsUpTo(int)} to change this value or set to true/false to always/never
     * count accurately.
     */
    public SearchRequestBuilder setTrackTotalHits(boolean trackTotalHits) {
        sourceBuilder().trackTotalHits(trackTotalHits);
        return this;
    }

    /**
     * Indicates the total hit count that should be tracked accurately or null if the value is unset. Defaults to 10,000.
     */
    public SearchRequestBuilder setTrackTotalHitsUpTo(int trackTotalHitsUpTo) {
        sourceBuilder().trackTotalHitsUpTo(trackTotalHitsUpTo);
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public SearchRequestBuilder addAggregation(AggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    public SearchRequestBuilder highlighter(HighlightBuilder highlightBuilder) {
        sourceBuilder().highlighter(highlightBuilder);
        return this;
    }

    /**
     * Clears all rescorers on the builder and sets the first one.  To use multiple rescore windows use
     * {@code #addRescorer(org.codelibs.fesen.opensearch.search.rescore.RescorerBuilder, int)}.
     *
     * @param rescorer rescorer configuration
     * @param window   rescore window
     * @return this for chaining
     */
    public SearchRequestBuilder setRescorer(RescorerBuilder rescorer, int window) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer.windowSize(window));
    }

    /**
     * Adds a new rescorer.
     *
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public SearchRequestBuilder addRescorer(RescorerBuilder<?> rescorer) {
        sourceBuilder().addRescorer(rescorer);
        return this;
    }

    public SearchRequestBuilder setCollapse(CollapseBuilder collapse) {
        sourceBuilder().collapse(collapse);
        return this;
    }

    /**
     * If specified, OpenSearch will execute this search request using reader contexts from that point in time.
     */
    public SearchRequestBuilder setPointInTime(PointInTimeBuilder pointInTimeBuilder) {
        sourceBuilder().pointInTimeBuilder(pointInTimeBuilder);
        return this;
    }

    @Override
    public String toString() {
        if (request.source() != null) {
            return request.source().toString();
        }
        return new SearchSourceBuilder().toString();
    }

    private SearchSourceBuilder sourceBuilder() {
        if (request.source() == null) {
            request.source(new SearchSourceBuilder());
        }
        return request.source();
    }
}
