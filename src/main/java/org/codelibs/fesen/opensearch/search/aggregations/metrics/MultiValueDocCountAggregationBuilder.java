/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Map;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSource;

/**
 * Aggregation Builder for multivalue_doc_count agg
 *
 * @opensearch.internal
 */
public class MultiValueDocCountAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<
    ValuesSource,
    MultiValueDocCountAggregationBuilder> {

    public static final String NAME = "multivalue_doc_count";
    public static final ObjectParser<MultiValueDocCountAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        MultiValueDocCountAggregationBuilder::new
    );

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
    }

    protected MultiValueDocCountAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Read from a stream.
     */
    public MultiValueDocCountAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    protected MultiValueDocCountAggregationBuilder(
        MultiValueDocCountAggregationBuilder clone,
        AggregatorFactories.Builder factoryBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoryBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MultiValueDocCountAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

}
