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

import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.CompositeIndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions.WildcardStates;
import org.codelibs.fesen.opensearch.common.CheckedBiConsumer;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.tasks.CancellableTask;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi search API request.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MultiSearchRequest extends ActionRequest implements CompositeIndicesRequest {

    public static final int MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT = 0;

    private int maxConcurrentSearchRequests = 0;
    private final List<SearchRequest> requests = new ArrayList<>();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    public MultiSearchRequest() {}

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequestBuilder request) {
        requests.add(request.request());
        return this;
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Returns the amount of search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public int maxConcurrentSearchRequests() {
        return maxConcurrentSearchRequests;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchRequest maxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        if (maxConcurrentSearchRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentSearchRequests must be positive");
        }

        this.maxConcurrentSearchRequests = maxConcurrentSearchRequests;
        return this;
    }

    public List<SearchRequest> requests() {
        return this.requests;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (int i = 0; i < requests.size(); i++) {
            ActionRequestValidationException ex = requests.get(i).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public MultiSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public MultiSearchRequest(StreamInput in) throws IOException {
        super(in);
        maxConcurrentSearchRequests = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(maxConcurrentSearchRequests);
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            request.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiSearchRequest that = (MultiSearchRequest) o;
        return maxConcurrentSearchRequests == that.maxConcurrentSearchRequests
            && Objects.equals(requests, that.requests)
            && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxConcurrentSearchRequests, requests, indicesOptions);
    }

    public static void writeSearchRequestParams(SearchRequest request, XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();
        if (request.indices() != null) {
            xContentBuilder.field("index", request.indices());
        }
        if (request.indicesOptions() != null && request.indicesOptions() != SearchRequest.DEFAULT_INDICES_OPTIONS) {
            WildcardStates.toXContent(request.indicesOptions().getExpandWildcards(), xContentBuilder);
            xContentBuilder.field("ignore_unavailable", request.indicesOptions().ignoreUnavailable());
            xContentBuilder.field("allow_no_indices", request.indicesOptions().allowNoIndices());
        }
        if (request.searchType() != null) {
            xContentBuilder.field("search_type", request.searchType().name().toLowerCase(Locale.ROOT));
        }
        xContentBuilder.field("ccs_minimize_roundtrips", request.isCcsMinimizeRoundtrips());
        if (request.requestCache() != null) {
            xContentBuilder.field("request_cache", request.requestCache());
        }
        if (request.preference() != null) {
            xContentBuilder.field("preference", request.preference());
        }
        if (request.routing() != null) {
            xContentBuilder.field("routing", request.routing());
        }
        if (request.allowPartialSearchResults() != null) {
            xContentBuilder.field("allow_partial_search_results", request.allowPartialSearchResults());
        }
        if (request.getCancelAfterTimeInterval() != null) {
            xContentBuilder.field("cancel_after_time_interval", request.getCancelAfterTimeInterval().getStringRep());
        }
        if (request.isPhaseTook() != null) {
            xContentBuilder.field("phase_took", request.isPhaseTook());
        }
        xContentBuilder.endObject();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return requests.stream().map(SearchRequest::buildDescription).collect(Collectors.joining(action + "[", ",", "]"));
            }

            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public String toString() {
        return "MultiSearchRequest{"
            + "maxConcurrentSearchRequests="
            + maxConcurrentSearchRequests
            + ", requests="
            + requests
            + ", indicesOptions="
            + indicesOptions
            + '}';
    }
}
