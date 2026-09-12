/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRoutingService;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for deleting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportDeleteWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterDeleteWeightedRoutingRequest,
    ClusterDeleteWeightedRoutingResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteWeightedRoutingAction.class);

    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportDeleteWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterDeleteWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterDeleteWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterDeleteWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterDeleteWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterDeleteWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterDeleteWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterDeleteWeightedRoutingResponse> listener
    ) throws Exception {
        weightedRoutingService.deleteWeightedRoutingMetadata(request, listener);
    }
}
