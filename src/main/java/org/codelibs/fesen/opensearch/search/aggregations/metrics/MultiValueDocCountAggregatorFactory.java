/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.index.query.QueryShardContext;
import org.codelibs.fesen.opensearch.search.aggregations.Aggregator;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.opensearch.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.codelibs.fesen.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation Factory for multivalue_doc_count agg
 *
 * @opensearch.internal
 */
public class MultiValueDocCountAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MultiValueDocCountAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.ALL_CORE,
            MultiValueDocCountAggregator::new,
            true
        );
    }

    MultiValueDocCountAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new MultiValueDocCountAggregator(name, config, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(MultiValueDocCountAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, searchContext, parent, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
