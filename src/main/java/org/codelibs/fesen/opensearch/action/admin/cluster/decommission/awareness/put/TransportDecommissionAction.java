/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.put;

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
 * Transport action for registering decommission
 *
 * @opensearch.internal
 */
public class TransportDecommissionAction extends TransportClusterManagerNodeAction<DecommissionRequest, DecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDecommissionAction.class);
    private final DecommissionService decommissionService;

    public TransportDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DecommissionRequest::new,
            indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DecommissionResponse read(StreamInput in) throws IOException {
        return new DecommissionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(DecommissionRequest request, ClusterState state, ActionListener<DecommissionResponse> listener)
        throws Exception {
        logger.info("starting awareness attribute [{}] decommissioning", request.getDecommissionAttribute().toString());
        decommissionService.startDecommissionAction(request, listener);
    }
}
