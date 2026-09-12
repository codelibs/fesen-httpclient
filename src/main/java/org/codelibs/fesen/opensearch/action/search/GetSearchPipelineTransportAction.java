/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineService;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Perform the action of getting a search pipeline
 *
 * @opensearch.internal
 */
public class GetSearchPipelineTransportAction extends TransportClusterManagerNodeReadAction<
    GetSearchPipelineRequest,
    GetSearchPipelineResponse> {

    @Inject
    public GetSearchPipelineTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSearchPipelineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSearchPipelineRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSearchPipelineResponse read(StreamInput in) throws IOException {
        return new GetSearchPipelineResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetSearchPipelineRequest request,
        ClusterState state,
        ActionListener<GetSearchPipelineResponse> listener
    ) throws Exception {
        listener.onResponse(new GetSearchPipelineResponse(SearchPipelineService.getPipelines(state, request.getIds())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
