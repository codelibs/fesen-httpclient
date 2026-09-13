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

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.search.Scroll;
import org.codelibs.fesen.opensearch.search.builder.PointInTimeBuilder;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.opensearch.search.internal.SearchContext;
import org.codelibs.fesen.opensearch.search.sort.FieldSortBuilder;
import org.codelibs.fesen.opensearch.search.sort.ShardDocSortBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilder;
import org.codelibs.fesen.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link Requests#searchRequest(String...)}.
 * <p>
 * Note, the search {@link #source(org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including aggregations and such.
 * </p>
 *
 * @see Requests#searchRequest(String...)
 * @see Client#search(SearchRequest)
 * @see SearchResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchRequest extends ActionRequest implements IndicesRequest.Replaceable {

    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 128;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 512;

    private static final long DEFAULT_ABSOLUTE_START_MILLIS = -1;

    private final String localClusterAlias;
    private final long absoluteStartMillis;
    private final boolean finalReduce;

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices = Strings.EMPTY_ARRAY;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private SearchSourceBuilder source;

    private Boolean requestCache;

    private Boolean allowPartialSearchResults;

    private Scroll scroll;

    private int batchedReduceSize = DEFAULT_BATCHED_REDUCE_SIZE;

    private int maxConcurrentShardRequests = 0;

    private Integer preFilterShardSize;

    private boolean ccsMinimizeRoundtrips = true;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    private TimeValue cancelAfterTimeInterval;

    private String pipeline;

    private Boolean phaseTook = null;

    public SearchRequest() {
        this.localClusterAlias = null;
        this.absoluteStartMillis = DEFAULT_ABSOLUTE_START_MILLIS;
        this.finalReduce = true;
    }

    /**
     * Constructs a new search request from the provided search request
     */
    public SearchRequest(SearchRequest searchRequest) {
        this(
            searchRequest,
            searchRequest.indices,
            searchRequest.localClusterAlias,
            searchRequest.absoluteStartMillis,
            searchRequest.finalReduce
        );
    }

    /**
     * Constructs a new search request against the indices. No indices provided here means that search
     * will run against all indices.
     */
    public SearchRequest(String... indices) {
        this(indices, new SearchSourceBuilder());
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public SearchRequest(String[] indices, SearchSourceBuilder source) {
        this();
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        indices(indices);
        this.source = source;
    }

    private SearchRequest(
        SearchRequest searchRequest,
        String[] indices,
        String localClusterAlias,
        long absoluteStartMillis,
        boolean finalReduce
    ) {
        this.allowPartialSearchResults = searchRequest.allowPartialSearchResults;
        this.batchedReduceSize = searchRequest.batchedReduceSize;
        this.ccsMinimizeRoundtrips = searchRequest.ccsMinimizeRoundtrips;
        this.indices = indices;
        this.indicesOptions = searchRequest.indicesOptions;
        this.maxConcurrentShardRequests = searchRequest.maxConcurrentShardRequests;
        this.preference = searchRequest.preference;
        this.preFilterShardSize = searchRequest.preFilterShardSize;
        this.requestCache = searchRequest.requestCache;
        this.routing = searchRequest.routing;
        this.scroll = searchRequest.scroll;
        this.searchType = searchRequest.searchType;
        this.source = searchRequest.source;
        this.localClusterAlias = localClusterAlias;
        this.absoluteStartMillis = absoluteStartMillis;
        this.finalReduce = finalReduce;
        this.cancelAfterTimeInterval = searchRequest.cancelAfterTimeInterval;
        this.phaseTook = searchRequest.phaseTook;
    }

    /**
     * Constructs a new search request from reading the specified stream.
     *
     * @param in The stream the request is read from
     * @throws IOException if there is an issue reading the stream
     */
    public SearchRequest(StreamInput in) throws IOException {
        super(in);
        searchType = SearchType.fromId(in.readByte());
        indices = in.readStringArray();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        if (in.getVersion().before(Version.V_2_0_0)) {
            // types no longer relevant so ignore
            String[] types = in.readStringArray();
            if (types.length > 0) {
                throw new IllegalStateException(
                    "types are no longer supported in search requests but found [" + Arrays.toString(types) + "]"
                );
            }
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        requestCache = in.readOptionalBoolean();
        batchedReduceSize = in.readVInt();
        maxConcurrentShardRequests = in.readVInt();
        preFilterShardSize = in.readOptionalVInt();
        allowPartialSearchResults = in.readOptionalBoolean();
        localClusterAlias = in.readOptionalString();
        if (localClusterAlias != null) {
            absoluteStartMillis = in.readVLong();
            finalReduce = in.readBoolean();
        } else {
            absoluteStartMillis = DEFAULT_ABSOLUTE_START_MILLIS;
            finalReduce = true;
        }
        ccsMinimizeRoundtrips = in.readBoolean();
        cancelAfterTimeInterval = in.readOptionalTimeValue();
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            pipeline = in.readOptionalString();
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            phaseTook = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(searchType.id());
        out.writeStringArray(indices);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        if (out.getVersion().before(Version.V_2_0_0)) {
            // types not supported so send an empty array to previous versions
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalBoolean(requestCache);
        out.writeVInt(batchedReduceSize);
        out.writeVInt(maxConcurrentShardRequests);
        out.writeOptionalVInt(preFilterShardSize);
        out.writeOptionalBoolean(allowPartialSearchResults);
        out.writeOptionalString(localClusterAlias);
        if (localClusterAlias != null) {
            out.writeVLong(absoluteStartMillis);
            out.writeBoolean(finalReduce);
        }
        out.writeBoolean(ccsMinimizeRoundtrips);
        out.writeOptionalTimeValue(cancelAfterTimeInterval);
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeOptionalString(pipeline);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalBoolean(phaseTook);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        boolean scroll = scroll() != null;
        if (scroll) {
            if (source != null) {
                if (source.trackTotalHitsUpTo() != null && source.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                    validationException = addValidationError(
                        "disabling [track_total_hits] is not allowed in a scroll context",
                        validationException
                    );
                }
                if (source.from() > 0) {
                    validationException = addValidationError("using [from] is not allowed in a scroll context", validationException);
                }
                if (source.size() == 0) {
                    validationException = addValidationError("[size] cannot be [0] in a scroll context", validationException);
                }
                if (source.rescores() != null && source.rescores().isEmpty() == false) {
                    validationException = addValidationError("using [rescore] is not allowed in a scroll context", validationException);
                }
            }
            if (requestCache != null && requestCache) {
                validationException = addValidationError("[request_cache] cannot be used in a scroll context", validationException);
            }
        }
        if (source != null) {
            if (source.aggregations() != null) {
                validationException = source.aggregations().validate(validationException);
            }
        }
        if (pointInTimeBuilder() != null) {
            if (scroll) {
                validationException = addValidationError("using [point in time] is not allowed in a scroll context", validationException);
            }
        }

        // _shard_doc validation
        if (source != null && source.sorts() != null && !source.sorts().isEmpty()) {
            int shardDocCount = 0;
            for (SortBuilder<?> sb : source.sorts()) {
                if (isShardDocSort(sb)) shardDocCount++;
            }
            final boolean hasPit = pointInTimeBuilder() != null;

            if (shardDocCount > 0 && scroll) {
                validationException = addValidationError(
                    "_shard_doc cannot be used with scroll. Use PIT + search_after instead.",
                    validationException
                );
            }
            if (shardDocCount > 0 && !hasPit) {
                validationException = addValidationError(
                    "_shard_doc is only supported with point-in-time (PIT). Add a PIT or remove _shard_doc.",
                    validationException
                );
            }
            if (shardDocCount > 1) {
                validationException = addValidationError(
                    "duplicate _shard_doc sort detected. Specify it at most once.",
                    validationException
                );
            }
        }
        return validationException;
    }

    private static boolean isShardDocSort(SortBuilder<?> sb) {
        if (sb instanceof ShardDocSortBuilder) return true;
        if (sb instanceof FieldSortBuilder) {
            return ShardDocSortBuilder.NAME.equals(((FieldSortBuilder) sb).getFieldName());
        }
        return false;
    }

    /**
     * Returns the alias of the cluster that this search request is being executed on. A non-null value indicates that this search request
     * is being executed as part of a locally reduced cross-cluster search request. The cluster alias is used to prefix index names
     * returned as part of search hits with the alias of the cluster they came from.
     */
    @Nullable
    String getLocalClusterAlias() {
        return localClusterAlias;
    }

    /**
     * Returns whether the reduction phase that will be performed needs to be final or not.
     */
    boolean isFinalReduce() {
        return finalReduce;
    }

    /**
     * Returns the current time in milliseconds from the time epoch, to be used for the execution of this search request. Used to
     * ensure that the same value, determined by the coordinating node, is used on all nodes involved in the execution of the search
     * request. When created through {@link #subSearchRequest(SearchRequest, String[], String, long, boolean)}, this method returns
     * the provided current time, otherwise it will return {@link System#currentTimeMillis()}.
     */
    public long getOrCreateAbsoluteStartMillis() {
        return absoluteStartMillis == DEFAULT_ABSOLUTE_START_MILLIS ? System.currentTimeMillis() : absoluteStartMillis;
    }

    /**
     * Returns the provided <code>absoluteStartMillis</code> when created through {@link #subSearchRequest} and
     * -1 otherwise.
     */
    long getAbsoluteStartMillis() {
        return absoluteStartMillis;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public SearchRequest indices(String... indices) {
        validateIndices(indices);
        this.indices = indices;
        return this;
    }

    private static void validateIndices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public SearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns whether network round-trips should be minimized when executing cross-cluster search requests.
     * Defaults to <code>true</code>.
     */
    public boolean isCcsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public SearchRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public SearchRequest searchType(SearchType searchType) {
        this.searchType = Objects.requireNonNull(searchType, "searchType must not be null");
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequest searchType(String searchType) {
        return searchType(SearchType.fromString(searchType));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        this.source = Objects.requireNonNull(sourceBuilder, "source must not be null");
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchSourceBuilder source() {
        return source;
    }

    public PointInTimeBuilder pointInTimeBuilder() {
        if (source != null) {
            return source.pointInTimeBuilder();
        }
        return null;
    }

    /**
     * The type of search to execute.
     */
    public SearchType searchType() {
        return searchType;
    }

    /**
     * The indices
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public Scroll scroll() {
        return scroll;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest scroll(TimeValue keepAlive) {
        return scroll(new Scroll(keepAlive));
    }

    public Boolean requestCache() {
        return this.requestCache;
    }

    public Boolean allowPartialSearchResults() {
        return this.allowPartialSearchResults;
    }

    /**
     * Returns the number of shard results that should be reduced at once on the coordinating node. This value should be used as a
     * protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public int getBatchedReduceSize() {
        return batchedReduceSize;
    }

    /**
     * Returns the number of shard requests that should be executed concurrently on a single node. This value should be used as a
     * protection mechanism to reduce the number of shard requests fired per high level search request. Searches that hit the entire
     * cluster can be throttled with this number to reduce the cluster load. The default is {@code 5}
     */
    public int getMaxConcurrentShardRequests() {
        return maxConcurrentShardRequests == 0 ? 5 : maxConcurrentShardRequests;
    }

    /**
     * Returns the raw value of maxConcurrentShardRequests without applying the default.
     * A value of {@code 0} means the user has not explicitly set this parameter.
     */
    @ExperimentalApi
    public int getMaxConcurrentShardRequestsRaw() {
        return maxConcurrentShardRequests;
    }

    /**
     * Returns value of user-provided phase_took query parameter for this search request.
     */
    public Boolean isPhaseTook() {
        return phaseTook;
    }

    /**
     * Returns a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold, or <code>null</code> if the threshold is unspecified.
     * This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on its rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint.
     * <p>
     * When unspecified, the pre-filter phase is executed if any of these conditions is met:
     * <ul>
     * <li>The request targets more than 128 shards</li>
     * <li>The request targets one or more read-only index</li>
     * <li>The primary sort of the query targets an indexed field</li>
     * </ul>
     */
    @Nullable
    public Integer getPreFilterShardSize() {
        return preFilterShardSize;
    }

    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    public String pipeline() {
        return pipeline;
    }

    public final String buildDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("indices[");
        Strings.arrayToDelimitedString(indices, ",", sb);
        sb.append("], ");
        sb.append("search_type[").append(searchType).append("], ");
        if (scroll != null) {
            sb.append("scroll[").append(scroll.keepAlive()).append("], ");
        }
        if (source != null) {
            sb.append("source[");
            try {
                sb.append(source.toString(FORMAT_PARAMS));
            } catch (final OpenSearchException ex) {
                sb.append("<error: ").append(ex.getMessage()).append(">");
            }
            sb.append("]");
        } else {
            sb.append("source[]");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchRequest that = (SearchRequest) o;
        return searchType == that.searchType
            && Arrays.equals(indices, that.indices)
            && Objects.equals(routing, that.routing)
            && Objects.equals(preference, that.preference)
            && Objects.equals(source, that.source)
            && Objects.equals(requestCache, that.requestCache)
            && Objects.equals(scroll, that.scroll)
            && Objects.equals(batchedReduceSize, that.batchedReduceSize)
            && Objects.equals(maxConcurrentShardRequests, that.maxConcurrentShardRequests)
            && Objects.equals(preFilterShardSize, that.preFilterShardSize)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(allowPartialSearchResults, that.allowPartialSearchResults)
            && Objects.equals(localClusterAlias, that.localClusterAlias)
            && absoluteStartMillis == that.absoluteStartMillis
            && ccsMinimizeRoundtrips == that.ccsMinimizeRoundtrips
            && Objects.equals(cancelAfterTimeInterval, that.cancelAfterTimeInterval)
            && Objects.equals(pipeline, that.pipeline)
            && Objects.equals(phaseTook, that.phaseTook);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            searchType,
            Arrays.hashCode(indices),
            routing,
            preference,
            source,
            requestCache,
            scroll,
            indicesOptions,
            batchedReduceSize,
            maxConcurrentShardRequests,
            preFilterShardSize,
            allowPartialSearchResults,
            localClusterAlias,
            absoluteStartMillis,
            ccsMinimizeRoundtrips,
            cancelAfterTimeInterval,
            phaseTook
        );
    }

    @Override
    public String toString() {
        return "SearchRequest{"
            + "searchType="
            + searchType
            + ", indices="
            + Arrays.toString(indices)
            + ", indicesOptions="
            + indicesOptions
            + ", routing='"
            + routing
            + '\''
            + ", preference='"
            + preference
            + '\''
            + ", requestCache="
            + requestCache
            + ", scroll="
            + scroll
            + ", maxConcurrentShardRequests="
            + maxConcurrentShardRequests
            + ", batchedReduceSize="
            + batchedReduceSize
            + ", preFilterShardSize="
            + preFilterShardSize
            + ", allowPartialSearchResults="
            + allowPartialSearchResults
            + ", localClusterAlias="
            + localClusterAlias
            + ", getOrCreateAbsoluteStartMillis="
            + absoluteStartMillis
            + ", ccsMinimizeRoundtrips="
            + ccsMinimizeRoundtrips
            + ", source="
            + source
            + ", cancelAfterTimeInterval="
            + cancelAfterTimeInterval
            + ", pipeline="
            + pipeline
            + ", phaseTook="
            + phaseTook
            + "}";
    }
}
