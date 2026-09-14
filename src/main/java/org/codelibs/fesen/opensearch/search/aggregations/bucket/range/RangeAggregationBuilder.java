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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.range;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation Builder for range agg
 *
 * @opensearch.internal
 */
public class RangeAggregationBuilder extends AbstractRangeBuilder<RangeAggregationBuilder, Range> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "range";
    /**
     * The PARSER constant.
     */
    public static final ObjectParser<RangeAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, RangeAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareBoolean(RangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) {
                agg.addRange(range);
            }
        }, (p, c) -> RangeAggregator.Range.PARSER.parse(p, null), RangeAggregator.RANGES_FIELD);
    }

    /**
     * Creates a new RangeAggregationBuilder.
     *
     * @param name the name
     */
    public RangeAggregationBuilder(String name) {
        super(name, InternalRange.FACTORY);
    }

    /**
     * Creates a new RangeAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected RangeAggregationBuilder(
        RangeAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @param to
     *            the upper bound on the distances, exclusive
     * @return this instance
     */
    public RangeAggregationBuilder addRange(String key, double from, double to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and
     * <code>to</code>.
     *
     * @param from the offset
     * @param to the target
     * @return this instance
     */
    public RangeAggregationBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the distances, exclusive
     * @return this instance
     */
    public RangeAggregationBuilder addUnboundedTo(String key, double to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     *
     * @param to the target
     * @return this instance
     */
    public RangeAggregationBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @return this instance
     */
    public RangeAggregationBuilder addUnboundedFrom(String key, double from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     *
     * @param from the offset
     * @return this instance
     */
    public RangeAggregationBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    public String getType() {
        return NAME;
    }

}
