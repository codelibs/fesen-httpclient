/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.storage.action.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.storage.tiering.TieringService;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Base transport handler for tiering.
 */
public class TransportTierAction extends TransportClusterManagerNodeAction<IndexTieringRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportTierAction.class);
    private final TieringService tieringService;

    /**
     * Constructs a TransportTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param migrationActionName the action name for this tiering operation
     * @param tieringService the tiering service
     */
    public TransportTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        String migrationActionName,
        TieringService tieringService
    ) {
        super(
            migrationActionName,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndexTieringRequest::new,
            indexNameExpressionResolver
        );
        this.tieringService = tieringService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(IndexTieringRequest request, ClusterState state) {
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException == null) {
            blockException = state.blocks().globalBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return blockException != null ? blockException : state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected void clusterManagerOperation(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        ActionListener<ClusterStateUpdateResponse> clusterStateUpdateListener = new ActionListener<>() {
            /**
             * Handle action response. This response may constitute a failure or a
             * success, but it is up to the listener to make that decision.
             * @param clusterStateUpdateResponse
             */
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new AcknowledgedResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            /**
             * A failure caused by an exception at some phase of the task.
             * @param e
             */
            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> new ParameterizedMessage("failed to start tiering for index [{}]", request.getIndex()), e);
                listener.onFailure(e);
            }
        };
        tieringService.tier(request, clusterStateUpdateListener, state);
    }

}
