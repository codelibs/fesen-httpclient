/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.index.compositeindex.datacube.MetricStat;
import org.codelibs.fesen.opensearch.index.query.QueryShardContext;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * Extending ValuesSourceAggregatorFactory for aggregation factories supported by star-tree implementation
 */
public abstract class MetricAggregatorFactory extends ValuesSourceAggregatorFactory {
    public MetricAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    public abstract MetricStat getMetricStat();
}
