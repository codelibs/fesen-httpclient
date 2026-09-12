/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
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
 * Transport action for updating weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportAddWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse> {

    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportAddWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterAddWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterPutWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterPutWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterPutWeightedRoutingResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterPutWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterPutWeightedRoutingResponse> listener
    ) throws Exception {
        try {
            weightedRoutingService.verifyAwarenessAttribute(request.getWeightedRouting().attributeName());
        } catch (ActionRequestValidationException ex) {
            listener.onFailure(ex);
            return;
        }
        weightedRoutingService.registerWeightedRoutingMetadata(
            request,
            ActionListener.delegateFailure(listener, (delegatedListener, response) -> {
                delegatedListener.onResponse(new ClusterPutWeightedRoutingResponse(response.isAcknowledged()));
            })
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterPutWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
