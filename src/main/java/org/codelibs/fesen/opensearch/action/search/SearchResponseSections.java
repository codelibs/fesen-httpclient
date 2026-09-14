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

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.SearchExtBuilder;
import org.codelibs.fesen.opensearch.search.SearchHits;
import org.codelibs.fesen.opensearch.search.aggregations.Aggregations;
import org.codelibs.fesen.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.codelibs.fesen.opensearch.search.profile.ProfileShardResult;
import org.codelibs.fesen.opensearch.search.profile.SearchProfileShardResults;
import org.codelibs.fesen.opensearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class that holds the various sections which a search response is
 * composed of (hits, aggs, suggestions etc.) and allows to retrieve them.
 * <p>
 * The reason why this class exists is that the high level REST client uses its own classes
 * to parse aggregations into, which are not serializable. This is the common part that can be
 * shared between core and client.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchResponseSections implements ToXContentFragment {

    /**
     * The EXT_FIELD constant.
     */
    public static final ParseField EXT_FIELD = new ParseField("ext");
    /**
     * The PROCESSOR_RESULT_FIELD constant.
     */
    public static final ParseField PROCESSOR_RESULT_FIELD = new ParseField("processor_results");
    /**
     * The hits.
     */
    protected final SearchHits hits;
    /**
     * The aggregations.
     */
    protected final Aggregations aggregations;
    /**
     * The suggest.
     */
    protected final Suggest suggest;
    /**
     * The profile results.
     */
    protected final SearchProfileShardResults profileResults;
    /**
     * The timed out.
     */
    protected final boolean timedOut;
    /**
     * The terminated early.
     */
    protected final Boolean terminatedEarly;
    /**
     * The num reduce phases.
     */
    protected final int numReducePhases;
    /**
     * The search ext builders.
     */
    protected final List<SearchExtBuilder> searchExtBuilders = new ArrayList<>();
    /**
     * The processor result.
     */
    protected final List<ProcessorExecutionDetail> processorResult = new ArrayList<>();

    /**
     * Creates a new SearchResponseSections.
     *
     * @param hits the hits
     * @param aggregations the aggregations
     * @param suggest the suggest
     * @param timedOut the timed out
     * @param terminatedEarly the terminated early
     * @param profileResults the profile results
     * @param numReducePhases the num reduce phases
     * @param searchExtBuilders the search ext builders
     * @param processorResult the processor result
     */
    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileShardResults profileResults,
        int numReducePhases,
        List<SearchExtBuilder> searchExtBuilders,
        List<ProcessorExecutionDetail> processorResult
    ) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
        this.processorResult.addAll(processorResult);
        this.searchExtBuilders.addAll(Objects.requireNonNull(searchExtBuilders, "searchExtBuilders must not be null"));
    }

    /**
     * Returns the timed out.
     *
     * @return the timed out
     */
    public final boolean timedOut() {
        return this.timedOut;
    }

    /**
     * Returns the terminated early.
     *
     * @return the terminated early
     */
    public final Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    /**
     * Returns the hits.
     *
     * @return the hits
     */
    public final SearchHits hits() {
        return hits;
    }

    /**
     * Returns the aggregations.
     *
     * @return the aggregations
     */
    public final Aggregations aggregations() {
        return aggregations;
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     *
     * @return the num reduce phases
     */
    public final int getNumReducePhases() {
        return numReducePhases;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        if (suggest != null) {
            suggest.toXContent(builder, params);
        }
        if (profileResults != null) {
            profileResults.toXContent(builder, params);
        }
        if (!searchExtBuilders.isEmpty()) {
            builder.startObject(EXT_FIELD.getPreferredName());
            for (SearchExtBuilder searchExtBuilder : searchExtBuilders) {
                searchExtBuilder.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (!processorResult.isEmpty()) {
            builder.field(PROCESSOR_RESULT_FIELD.getPreferredName(), processorResult);
        }
        return builder;
    }

    /**
     * Writes this instance to the given output.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    protected void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
