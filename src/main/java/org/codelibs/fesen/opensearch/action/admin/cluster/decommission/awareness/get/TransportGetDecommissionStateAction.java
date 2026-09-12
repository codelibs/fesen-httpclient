/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.get;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting decommission status
 *
 * @opensearch.internal
 */
public class TransportGetDecommissionStateAction extends TransportClusterManagerNodeReadAction<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse> {

    public TransportGetDecommissionStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDecommissionStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDecommissionStateRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDecommissionStateResponse read(StreamInput in) throws IOException {
        return new GetDecommissionStateResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetDecommissionStateRequest request,
        ClusterState state,
        ActionListener<GetDecommissionStateResponse> listener
    ) throws Exception {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null
            && request.attributeName().equals(decommissionAttributeMetadata.decommissionAttribute().attributeName())) {
            listener.onResponse(
                new GetDecommissionStateResponse(
                    decommissionAttributeMetadata.decommissionAttribute().attributeValue(),
                    decommissionAttributeMetadata.status()
                )
            );
        } else {
            listener.onResponse(new GetDecommissionStateResponse());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDecommissionStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
