/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.decommission.DecommissionService;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for delete decommission.
 *
 * @opensearch.internal
 */
public class TransportDeleteDecommissionStateAction extends TransportClusterManagerNodeAction<
    DeleteDecommissionStateRequest,
    DeleteDecommissionStateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDecommissionStateAction.class);
    private final DecommissionService decommissionService;

    public TransportDeleteDecommissionStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DecommissionService decommissionService
    ) {
        super(
            DeleteDecommissionStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDecommissionStateRequest::new,
            indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteDecommissionStateResponse read(StreamInput in) throws IOException {
        return new DeleteDecommissionStateResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDecommissionStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        DeleteDecommissionStateRequest request,
        ClusterState state,
        ActionListener<DeleteDecommissionStateResponse> listener
    ) {
        logger.info("Received delete decommission Request [{}]", request);
        this.decommissionService.startRecommissionAction(listener);
    }
}
