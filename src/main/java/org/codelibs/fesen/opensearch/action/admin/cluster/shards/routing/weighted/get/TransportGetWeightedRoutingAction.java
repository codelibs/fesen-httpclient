/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRouting;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRoutingService;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportGetWeightedRoutingAction extends TransportClusterManagerNodeReadAction<
    ClusterGetWeightedRoutingRequest,
    ClusterGetWeightedRoutingResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetWeightedRoutingAction.class);
    private final WeightedRoutingService weightedRoutingService;

    public TransportGetWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetWeightedRoutingRequest::new,
            indexNameExpressionResolver,
            true
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterGetWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterGetWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterGetWeightedRoutingRequest request,
        ClusterState state,
        final ActionListener<ClusterGetWeightedRoutingResponse> listener
    ) {
        try {
            weightedRoutingService.verifyAwarenessAttribute(request.getAwarenessAttribute());
            WeightedRoutingMetadata weightedRoutingMetadata = state.metadata().custom(WeightedRoutingMetadata.TYPE);
            ClusterGetWeightedRoutingResponse clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse();
            if (weightedRoutingMetadata != null && weightedRoutingMetadata.getWeightedRouting() != null) {
                WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
                clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse(
                    weightedRouting,
                    state.nodes().getClusterManagerNodeId() != null,
                    weightedRoutingMetadata.getVersion()
                );
            }
            listener.onResponse(clusterGetWeightedRoutingResponse);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
