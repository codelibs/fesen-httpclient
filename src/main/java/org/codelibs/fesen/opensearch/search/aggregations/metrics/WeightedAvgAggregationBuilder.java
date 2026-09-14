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

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.codelibs.fesen.opensearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.codelibs.fesen.opensearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;

/**
 * Aggregation Builder for weighted_avg agg
 *
 * @opensearch.internal
 */
public class WeightedAvgAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<WeightedAvgAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "weighted_avg";
    /**
     * The VALUE_FIELD constant.
     */
    public static final ParseField VALUE_FIELD = new ParseField("value");
    /**
     * The WEIGHT_FIELD constant.
     */
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    /**
     * The PARSER constant.
     */
    public static final ObjectParser<WeightedAvgAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        WeightedAvgAggregationBuilder::new
    );
    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(VALUE_FIELD.getPreferredName(), PARSER, true, false, false);
        MultiValuesSourceParseHelper.declareField(WEIGHT_FIELD.getPreferredName(), PARSER, true, false, false);
    }

    /**
     * Creates a new WeightedAvgAggregationBuilder.
     *
     * @param name the name
     */
    public WeightedAvgAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Creates a new WeightedAvgAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    public WeightedAvgAggregationBuilder(WeightedAvgAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    /**
     * Returns the value.
     *
     * @param valueConfig the value config
     * @return the value
     */
    public WeightedAvgAggregationBuilder value(MultiValuesSourceFieldConfig valueConfig) {
        valueConfig = Objects.requireNonNull(valueConfig, "Configuration for field [" + VALUE_FIELD + "] cannot be null");
        field(VALUE_FIELD.getPreferredName(), valueConfig);
        return this;
    }

    /**
     * Returns the weight.
     *
     * @param weightConfig the weight config
     * @return the weight
     */
    public WeightedAvgAggregationBuilder weight(MultiValuesSourceFieldConfig weightConfig) {
        weightConfig = Objects.requireNonNull(weightConfig, "Configuration for field [" + WEIGHT_FIELD + "] cannot be null");
        field(WEIGHT_FIELD.getPreferredName(), weightConfig);
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new WeightedAvgAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
