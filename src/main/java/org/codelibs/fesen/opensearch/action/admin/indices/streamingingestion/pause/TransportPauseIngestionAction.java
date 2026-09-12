/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.pause;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateResponse;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.DestructiveOperations;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataStreamingIngestionStateService;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Pause ingestion transport action.
 *
 * @opensearch.api
 */
public class TransportPauseIngestionAction extends TransportClusterManagerNodeAction<PauseIngestionRequest, PauseIngestionResponse>
    implements
        TransportIndicesResolvingAction<PauseIngestionRequest> {

    private static final Logger logger = LogManager.getLogger(TransportPauseIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportPauseIngestionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            PauseIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PauseIngestionRequest::new,
            indexNameExpressionResolver
        );
        this.ingestionStateService = ingestionStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PauseIngestionResponse read(StreamInput in) throws IOException {
        return new PauseIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, PauseIngestionRequest request, ActionListener<PauseIngestionResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PauseIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) throws Exception {
        final ResolvedIndices.Local.Concrete concreteIndices = resolveIndices(request, state);
        if (concreteIndices.concreteIndices().isEmpty()) {
            listener.onResponse(new PauseIngestionResponse(true, false, new IngestionStateShardFailure[0], ""));
            return;
        }

        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(
            concreteIndices.namesOfConcreteIndicesAsArray(),
            new int[0]
        );
        updateIngestionStateRequest.timeout(request.clusterManagerNodeTimeout());
        updateIngestionStateRequest.setIngestionPaused(true);

        ingestionStateService.updateIngestionPollerState(
            "pause-ingestion",
            concreteIndices.concreteIndicesAsArray(),
            updateIngestionStateRequest,
            new ActionListener<>() {

                @Override
                public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                    boolean shardsAcked = updateIngestionStateResponse.isAcknowledged()
                        && updateIngestionStateResponse.getFailedShards() == 0;
                    PauseIngestionResponse pauseIngestionResponse = new PauseIngestionResponse(
                        true,
                        shardsAcked,
                        updateIngestionStateResponse.getShardFailureList(),
                        updateIngestionStateResponse.getErrorMessage()
                    );
                    listener.onResponse(pauseIngestionResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Error pausing ingestion", e);
                    listener.onFailure(e);
                }
            }
        );
    }

    @Override
    public ResolvedIndices resolveIndices(PauseIngestionRequest request) {
        return ResolvedIndices.of(resolveIndices(request, clusterService.state()));
    }

    private ResolvedIndices.Local.Concrete resolveIndices(PauseIngestionRequest request, ClusterState clusterState) {
        return indexNameExpressionResolver.concreteResolvedIndices(clusterState, request);
    }
}
