/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.recovery;

import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.indices.cluster.IndicesClusterStateService;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationFailedException;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationListener;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationState;

/**
 * Listener that runs on changes in Recovery state
 *
 * @opensearch.internal
 */
@PublicApi(since = "2.2.0")
public class RecoveryListener implements ReplicationListener {

    /**
     * ShardRouting with which the shard was created
     */
    private final ShardRouting shardRouting;

    /**
     * Primary term with which the shard was created
     */
    private final long primaryTerm;

    private final IndicesClusterStateService indicesClusterStateService;

    public RecoveryListener(
        final ShardRouting shardRouting,
        final long primaryTerm,
        IndicesClusterStateService indicesClusterStateService
    ) {
        this.shardRouting = shardRouting;
        this.primaryTerm = primaryTerm;
        this.indicesClusterStateService = indicesClusterStateService;
    }

    @Override
    public void onDone(ReplicationState state) {
        indicesClusterStateService.handleRecoveryDone(state, shardRouting, primaryTerm);
    }

    @Override
    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
        indicesClusterStateService.handleRecoveryFailure(shardRouting, sendShardFailure, e);
    }
}
