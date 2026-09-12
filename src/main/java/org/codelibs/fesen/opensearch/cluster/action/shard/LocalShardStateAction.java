/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.action.shard;

import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.routing.IndexRoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.IndexShardRoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.RerouteService;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.AllocationService;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.util.function.Function;

/**
 * A local implementation of {@link ShardStateAction} that applies shard state changes directly to the
 * local cluster state. This is used in clusterless mode, where there is no cluster manager.
 */
public class LocalShardStateAction extends ShardStateAction {
    public LocalShardStateAction(
        ClusterService clusterService,
        TransportService transportService,
        AllocationService allocationService,
        RerouteService rerouteService,
        ThreadPool threadPool
    ) {
        super(clusterService, transportService, allocationService, rerouteService, threadPool);
    }

    @Override
    public void shardStarted(
        ShardRouting shardRouting,
        long primaryTerm,
        String message,
        ActionListener<Void> listener,
        ClusterState currentState
    ) {
        Function<ClusterState, ClusterState> clusterStateUpdater = clusterState -> {
            // We're running in clusterless mode. Apply the state change directly to the local cluster state.
            RoutingTable routingTable = clusterState.getRoutingTable();
            IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());

            ClusterState.Builder clusterStateBuilder = ClusterState.builder(clusterState);
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(routingTable);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(shardRouting.index());
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                if (indexShardRoutingTable.shardId().equals(shardRouting.shardId())) {
                    IndexShardRoutingTable.Builder indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(
                        indexShardRoutingTable
                    );
                    indexShardRoutingTableBuilder.removeShard(shardRouting);
                    indexShardRoutingTableBuilder.addShard(shardRouting.moveToStarted());
                    indexRoutingTableBuilder.addIndexShard(indexShardRoutingTableBuilder.build());
                } else {
                    indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable);
                }
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
            clusterStateBuilder.routingTable(routingTableBuilder.build());
            return clusterStateBuilder.build();
        };
        clusterService.getClusterApplierService()
            .updateClusterState("shard-started " + shardRouting.shardId(), clusterStateUpdater, (s, e) -> {});
    }

    @Override
    public void localShardFailed(
        ShardRouting shardRouting,
        String message,
        Exception failure,
        ActionListener<Void> listener,
        ClusterState currentState
    ) {
        // Do not send a failure to the cluster manager, as we are running in clusterless mode.
    }
}
