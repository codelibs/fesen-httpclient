/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.clone;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.snapshots.SnapshotsService;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for the clone snapshot operation.
 *
 * @opensearch.internal
 */
public final class TransportCloneSnapshotAction extends TransportClusterManagerNodeAction<CloneSnapshotRequest, AcknowledgedResponse> {

    private final SnapshotsService snapshotsService;

    public TransportCloneSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CloneSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CloneSnapshotRequest::new,
            indexNameExpressionResolver
        );
        this.snapshotsService = snapshotsService;
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
        CloneSnapshotRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        snapshotsService.executeClone(request, ActionListener.map(listener, v -> new AcknowledgedResponse(true)));
    }

    @Override
    protected ClusterBlockException checkBlock(CloneSnapshotRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
