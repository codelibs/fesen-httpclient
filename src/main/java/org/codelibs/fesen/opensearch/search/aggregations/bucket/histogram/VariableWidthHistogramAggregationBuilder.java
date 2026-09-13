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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;

/**
 * Aggregation Builder for variable_width_histogram agg
 *
 * @opensearch.internal
 */
public class VariableWidthHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<VariableWidthHistogramAggregationBuilder> {

    public static final String NAME = "variable_width_histogram";
    private static final ParseField NUM_BUCKETS_FIELD = new ParseField("buckets");

    private static final ParseField INITIAL_BUFFER_FIELD = new ParseField("initial_buffer");

    private static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");

    public static final ObjectParser<VariableWidthHistogramAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        VariableWidthHistogramAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setNumBuckets, NUM_BUCKETS_FIELD);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setShardSize, SHARD_SIZE_FIELD);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setInitialBuffer, INITIAL_BUFFER_FIELD);
    }

    private int numBuckets = 10;
    private int shardSize = -1;
    private int initialBuffer = -1;

    /** Create a new builder with the given name. */
    public VariableWidthHistogramAggregationBuilder(String name) {
        super(name);
    }

    protected VariableWidthHistogramAggregationBuilder(
        VariableWidthHistogramAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
        this.numBuckets = clone.numBuckets;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    public VariableWidthHistogramAggregationBuilder setNumBuckets(int numBuckets) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException(NUM_BUCKETS_FIELD.getPreferredName() + " must be greater than [0] for [" + name + "]");
        }
        this.numBuckets = numBuckets;
        return this;
    }

    public VariableWidthHistogramAggregationBuilder setShardSize(int shardSize) {
        if (shardSize <= 1) {
            // A shard size of 1 will cause divide by 0s and, even if it worked, would produce garbage results.
            throw new IllegalArgumentException(SHARD_SIZE_FIELD.getPreferredName() + " must be greater than [1] for [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    public VariableWidthHistogramAggregationBuilder setInitialBuffer(int initialBuffer) {
        if (initialBuffer <= 0) {
            throw new IllegalArgumentException(INITIAL_BUFFER_FIELD.getPreferredName() + " must be greater than [0] for [" + name + "]");
        }
        this.initialBuffer = initialBuffer;
        return this;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public int getShardSize() {
        if (shardSize == -1) {
            return numBuckets * 50;
        }
        return shardSize;
    }

    public int getInitialBuffer() {
        if (initialBuffer == -1) {
            return Math.min(10 * getShardSize(), 50000);
        }
        return initialBuffer;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new VariableWidthHistogramAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(numBuckets);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(NUM_BUCKETS_FIELD.getPreferredName(), numBuckets);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numBuckets, shardSize, initialBuffer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        VariableWidthHistogramAggregationBuilder other = (VariableWidthHistogramAggregationBuilder) obj;
        return Objects.equals(numBuckets, other.numBuckets)
            && Objects.equals(shardSize, other.shardSize)
            && Objects.equals(initialBuffer, other.initialBuffer);
    }

    @Override
    public String getType() {
        return NAME;
    }

}
