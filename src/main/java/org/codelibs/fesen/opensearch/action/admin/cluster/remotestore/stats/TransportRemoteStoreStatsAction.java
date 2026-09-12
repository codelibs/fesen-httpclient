/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.routing.PlainShardsIterator;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.routing.ShardsIterator;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.index.IndexService;
import org.codelibs.fesen.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.codelibs.fesen.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.codelibs.fesen.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.index.shard.ShardNotFoundException;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Encapsulates all remote store stats
 *
 * @opensearch.internal
 */
public class TransportRemoteStoreStatsAction extends TransportBroadcastByNodeAction<
    RemoteStoreStatsRequest,
    RemoteStoreStatsResponse,
    RemoteStoreStats> {

    private final IndicesService indicesService;

    private final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    public TransportRemoteStoreStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory
    ) {
        super(
            RemoteStoreStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            RemoteStoreStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
        this.remoteStoreStatsTrackerFactory = remoteStoreStatsTrackerFactory;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, RemoteStoreStatsRequest request, String[] concreteIndices) {
        final List<ShardRouting> newShardRoutings = new ArrayList<>();
        if (request.shards().length > 0) {
            clusterState.routingTable().allShards(concreteIndices).getShardRoutings().forEach(shardRouting -> {
                if (Arrays.asList(request.shards()).contains(Integer.toString(shardRouting.shardId().id()))) {
                    newShardRoutings.add(shardRouting);
                }
            });
        } else {
            newShardRoutings.addAll(clusterState.routingTable().allShards(concreteIndices).getShardRoutings());
        }
        return new PlainShardsIterator(
            newShardRoutings.stream()
                .filter(
                    shardRouting -> !request.local()
                        || (shardRouting.currentNodeId() == null
                            || shardRouting.currentNodeId().equals(clusterState.getNodes().getLocalNodeId()))
                )
                .filter(
                    shardRouting -> Boolean.parseBoolean(
                        clusterState.getMetadata().index(shardRouting.index()).getSettings().get(IndexMetadata.SETTING_REMOTE_STORE_ENABLED)
                    )
                )
                .collect(Collectors.toList())
        );
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RemoteStoreStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RemoteStoreStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected RemoteStoreStats readShardResult(StreamInput in) throws IOException {
        return new RemoteStoreStats(in);
    }

    @Override
    protected RemoteStoreStatsResponse newResponse(
        RemoteStoreStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<RemoteStoreStats> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new RemoteStoreStatsResponse(
            responses.toArray(new RemoteStoreStats[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected RemoteStoreStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new RemoteStoreStatsRequest(in);
    }

    @Override
    protected RemoteStoreStats shardOperation(RemoteStoreStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        RemoteSegmentTransferTracker remoteSegmentTransferTracker = remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(
            indexShard.shardId()
        );
        assert Objects.nonNull(remoteSegmentTransferTracker);
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = remoteStoreStatsTrackerFactory.getRemoteTranslogTransferTracker(
            indexShard.shardId()
        );
        assert Objects.nonNull(remoteTranslogTransferTracker);

        return new RemoteStoreStats(remoteSegmentTransferTracker.stats(), remoteTranslogTransferTracker.stats(), indexShard.routingEntry());
    }
}
