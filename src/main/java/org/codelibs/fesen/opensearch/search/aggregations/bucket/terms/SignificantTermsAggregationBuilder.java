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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.terms;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.index.query.QueryRewriteContext;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;

/**
 * Aggregation Builder for significant terms agg
 *
 * @opensearch.internal
 */
public class SignificantTermsAggregationBuilder extends ValuesSourceAggregationBuilder<SignificantTermsAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "significant_terms";
    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");
    static final ParseField HEURISTIC = new ParseField("significance_heuristic");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
        3,
        0,
        10,
        -1
    );
    static final SignificanceHeuristic DEFAULT_SIGNIFICANCE_HEURISTIC = new JLHScore();

    private static final ObjectParser<SignificantTermsAggregationBuilder, Void> PARSER = new ObjectParser<>(
        SignificantTermsAggregationBuilder.NAME,
        SignificanceHeuristic.class,
        SignificantTermsAggregationBuilder::significanceHeuristic,
        null
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareInt(SignificantTermsAggregationBuilder::shardSize, TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(SignificantTermsAggregationBuilder::minDocCount, TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(SignificantTermsAggregationBuilder::shardMinDocCount, TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(SignificantTermsAggregationBuilder::size, TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(SignificantTermsAggregationBuilder::executionHint, TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME);

        PARSER.declareObject(
            SignificantTermsAggregationBuilder::backgroundFilter,
            (p, context) -> parseInnerQueryBuilder(p),
            SignificantTermsAggregationBuilder.BACKGROUND_FILTER
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
            IncludeExclude::parseInclude,
            IncludeExclude.INCLUDE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );
    }

    /**
     * Parses this instance.
     *
     * @param aggregationName the aggregation name
     * @param parser the parser
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public static SignificantTermsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SignificantTermsAggregationBuilder(aggregationName), null);
    }

    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private QueryBuilder filterBuilder = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private SignificanceHeuristic significanceHeuristic = DEFAULT_SIGNIFICANCE_HEURISTIC;

    /**
     * Creates a new SignificantTermsAggregationBuilder.
     *
     * @param name the name
     */
    public SignificantTermsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Creates a new SignificantTermsAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected SignificantTermsAggregationBuilder(
        SignificantTermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.executionHint = clone.executionHint;
        this.filterBuilder = clone.filterBuilder;
        this.includeExclude = clone.includeExclude;
        this.significanceHeuristic = clone.significanceHeuristic;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected SignificantTermsAggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new SignificantTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        if (filterBuilder != null) {
            QueryBuilder rewrittenFilter = filterBuilder.rewrite(queryShardContext);
            if (rewrittenFilter != filterBuilder) {
                SignificantTermsAggregationBuilder rewritten = shallowCopy(factoriesBuilder, metadata);
                rewritten.backgroundFilter(rewrittenFilter);
                return rewritten;
            }
        }
        return super.doRewrite(queryShardContext);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalString(executionHint);
        out.writeOptionalNamedWriteable(filterBuilder);
        out.writeOptionalWriteable(includeExclude);
        out.writeNamedWriteable(significanceHeuristic);
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    /**
     * Returns the bucket count thresholds.
     *
     * @return the bucket count thresholds
     */
    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }

    /**
     * Buckets the count thresholds.
     *
     * @return this instance
     */
    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }

    /**
     * Buckets the count thresholds.
     *
     * @param bucketCountThresholds the bucket count thresholds
     * @return this instance
     */
    public SignificantTermsAggregationBuilder bucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        if (bucketCountThresholds == null) {
            throw new IllegalArgumentException("[bucketCountThresholds] must not be null: [" + name + "]");
        }
        this.bucketCountThresholds = bucketCountThresholds;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     *
     * @param size the size
     * @return the number of elements
     */
    public SignificantTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     *
     * @param shardSize the shard size
     * @return the shard size
     */
    public SignificantTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than  0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     *
     * @param minDocCount the min doc count
     * @return the min doc count
     */
    public SignificantTermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     *
     * @param shardMinDocCount the shard min doc count
     * @return the shard min doc count
     */
    public SignificantTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     *
     * @param executionHint the execution hint
     * @return the execution hint
     */
    public SignificantTermsAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: gets an execution hint to the aggregation.
     *
     * @return the execution hint
     */
    public String executionHint() {
        return executionHint;
    }

    /**
     * Returns the background filter.
     *
     * @param backgroundFilter the background filter
     * @return the background filter
     */
    public SignificantTermsAggregationBuilder backgroundFilter(QueryBuilder backgroundFilter) {
        if (backgroundFilter == null) {
            throw new IllegalArgumentException("[backgroundFilter] must not be null: [" + name + "]");
        }
        this.filterBuilder = backgroundFilter;
        return this;
    }

    /**
     * Returns the background filter.
     *
     * @return the background filter
     */
    public QueryBuilder backgroundFilter() {
        return filterBuilder;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     *
     * @param includeExclude the include exclude
     * @return this instance
     */
    public SignificantTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     *
     * @return this instance
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    /**
     * Returns the significance heuristic.
     *
     * @param significanceHeuristic the significance heuristic
     * @return the significance heuristic
     */
    public SignificantTermsAggregationBuilder significanceHeuristic(SignificanceHeuristic significanceHeuristic) {
        if (significanceHeuristic == null) {
            throw new IllegalArgumentException("[significanceHeuristic] must not be null: [" + name + "]");
        }
        this.significanceHeuristic = significanceHeuristic;
        return this;
    }

    /**
     * Returns the significance heuristic.
     *
     * @return the significance heuristic
     */
    public SignificanceHeuristic significanceHeuristic() {
        return significanceHeuristic;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        if (executionHint != null) {
            builder.field(TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        if (filterBuilder != null) {
            builder.field(BACKGROUND_FILTER.getPreferredName(), filterBuilder);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        significanceHeuristic.toXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketCountThresholds, executionHint, filterBuilder, includeExclude, significanceHeuristic);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        SignificantTermsAggregationBuilder other = (SignificantTermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
            && Objects.equals(executionHint, other.executionHint)
            && Objects.equals(filterBuilder, other.filterBuilder)
            && Objects.equals(includeExclude, other.includeExclude)
            && Objects.equals(significanceHeuristic, other.significanceHeuristic);
    }

    @Override
    public String getType() {
        return NAME;
    }

}
