/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.replication;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.indices.recovery.RecoverySettings;
import org.codelibs.fesen.opensearch.transport.TransportService;

/**
 * Factory to build {@link SegmentReplicationSource} used by {@link SegmentReplicationTargetService}.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceFactory {

    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    public SegmentReplicationSourceFactory(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public SegmentReplicationSource get(IndexShard shard) {
        if (shard.indexSettings().isAssignedOnRemoteNode()) {
            return new RemoteStoreReplicationSource(shard);
        } else {
            return new PrimaryShardReplicationSource(
                shard.recoveryState().getTargetNode(),
                shard.routingEntry().allocationId().getId(),
                transportService,
                recoverySettings,
                getPrimaryNode(shard.shardId())
            );
        }
    }

    private DiscoveryNode getPrimaryNode(ShardId shardId) {
        ShardRouting primaryShard = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        DiscoveryNode node = clusterService.state().nodes().get(primaryShard.currentNodeId());
        if (node == null) {
            throw new IllegalStateException("Cannot replicate, primary shard for " + shardId + " is not allocated on any node");
        }
        return node;
    }
}
