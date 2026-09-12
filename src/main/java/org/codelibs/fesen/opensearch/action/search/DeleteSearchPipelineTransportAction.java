/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineService;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Perform the action of deleting a search pipeline
 *
 * @opensearch.internal
 */
public class DeleteSearchPipelineTransportAction extends TransportClusterManagerNodeAction<
    DeleteSearchPipelineRequest,
    AcknowledgedResponse> {
    private final SearchPipelineService searchPipelineService;

    @Inject
    public DeleteSearchPipelineTransportAction(
        ThreadPool threadPool,
        SearchPipelineService searchPipelineService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DeleteSearchPipelineAction.NAME,
            transportService,
            searchPipelineService.getClusterService(),
            threadPool,
            actionFilters,
            DeleteSearchPipelineRequest::new,
            indexNameExpressionResolver
        );
        this.searchPipelineService = searchPipelineService;
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
    protected void clusterManagerOperation(
        DeleteSearchPipelineRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        searchPipelineService.deletePipeline(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
