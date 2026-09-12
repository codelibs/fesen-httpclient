/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.routing.GroupShardsIterator;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.opensearch.core.indices.breaker.CircuitBreakerService;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.search.SearchPhaseResult;
import org.codelibs.fesen.opensearch.search.SearchService;
import org.codelibs.fesen.opensearch.search.internal.AliasFilter;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineService;
import org.codelibs.fesen.opensearch.tasks.TaskResourceTrackingService;
import org.codelibs.fesen.opensearch.telemetry.metrics.MetricsRegistry;
import org.codelibs.fesen.opensearch.telemetry.tracing.Tracer;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.StreamTransportService;
import org.codelibs.fesen.opensearch.transport.Transport;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Transport search action for streaming search
 * @opensearch.internal
 */
public class StreamTransportSearchAction extends TransportSearchAction {
    @Inject
    public StreamTransportSearchAction(
        NodeClient client,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        @Nullable StreamTransportService transportService,
        SearchService searchService,
        @Nullable StreamSearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchPipelineService searchPipelineService,
        MetricsRegistry metricsRegistry,
        SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory,
        Tracer tracer,
        TaskResourceTrackingService taskResourceTrackingService,
        IndicesService indicesService
    ) {
        super(
            client,
            threadPool,
            circuitBreakerService,
            transportService,
            searchService,
            searchTransportService,
            searchPhaseController,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            namedWriteableRegistry,
            searchPipelineService,
            metricsRegistry,
            searchRequestOperationsCompositeListenerFactory,
            tracer,
            taskResourceTrackingService,
            indicesService
        );
    }

    AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction(
        SearchTask task,
        SearchRequest searchRequest,
        Executor executor,
        GroupShardsIterator<SearchShardIterator> shardIterators,
        SearchTimeProvider timeProvider,
        BiFunction<String, String, Transport.Connection> connectionLookup,
        ClusterState clusterState,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        ActionListener<SearchResponse> listener,
        boolean preFilter,
        ThreadPool threadPool,
        SearchResponse.Clusters clusters,
        SearchRequestContext searchRequestContext
    ) {
        if (preFilter) {
            throw new IllegalStateException("Search pre-filter is not supported in streaming");
        } else {
            final QueryPhaseResultConsumer queryResultConsumer = searchPhaseController.newStreamSearchPhaseResults(
                executor,
                circuitBreaker,
                task.getProgressListener(),
                searchRequest,
                shardIterators.size(),
                exc -> cancelTask(task, exc)
            );
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new StreamSearchQueryThenFetchAsyncAction(
                        logger,
                        searchTransportService,
                        connectionLookup,
                        aliasFilter,
                        concreteIndexBoosts,
                        indexRoutings,
                        searchPhaseController,
                        executor,
                        queryResultConsumer,
                        searchRequest,
                        listener,
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters,
                        searchRequestContext,
                        tracer
                    );
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
    }
}
