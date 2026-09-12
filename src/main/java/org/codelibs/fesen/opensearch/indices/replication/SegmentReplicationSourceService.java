/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.RateLimiter;
import org.codelibs.fesen.opensearch.action.support.ChannelActionListener;
import org.codelibs.fesen.opensearch.cluster.ClusterChangedEvent;
import org.codelibs.fesen.opensearch.cluster.ClusterStateListener;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.util.CancellableThreads;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.transport.TransportResponse;
import org.codelibs.fesen.opensearch.index.IndexService;
import org.codelibs.fesen.opensearch.index.shard.IndexEventListener;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.index.shard.IndexShardState;
import org.codelibs.fesen.opensearch.index.store.StoreFileMetadata;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.indices.recovery.MultiChunkTransfer;
import org.codelibs.fesen.opensearch.indices.recovery.RecoverySettings;
import org.codelibs.fesen.opensearch.indices.recovery.RetryableTransportClient;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationTimer;
import org.codelibs.fesen.opensearch.indices.replication.common.SegmentReplicationTransportRequest;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportChannel;
import org.codelibs.fesen.opensearch.transport.TransportRequestHandler;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Service class that handles segment replication requests from replica shards.
 * Typically, the "source" is a primary shard. This code executes on the source node.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationSourceService.class);
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;
    private final IndicesService indicesService;

    /**
     * Internal actions used by the segment replication source service on the primary shard
     *
     * @opensearch.internal
     */
    public static class Actions {

        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/replication/get_checkpoint_info";
        public static final String GET_SEGMENT_FILES = "internal:index/shard/replication/get_segment_files";
        public static final String UPDATE_VISIBLE_CHECKPOINT = "internal:index/shard/replication/update_visible_checkpoint";
        public static final String GET_MERGED_SEGMENT_FILES = "internal:index/shard/replication/get_merged_segment_files";
    }

    private final OngoingSegmentReplications ongoingSegmentReplications;

    protected SegmentReplicationSourceService(
        IndicesService indicesService,
        TransportService transportService,
        RecoverySettings recoverySettings,
        OngoingSegmentReplications ongoingSegmentReplications
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.ongoingSegmentReplications = ongoingSegmentReplications;
        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            CheckpointInfoRequest::new,
            new CheckpointInfoRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_SEGMENT_FILES,
            ThreadPool.Names.GENERIC,
            GetSegmentFilesRequest::new,
            new GetSegmentFilesRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.UPDATE_VISIBLE_CHECKPOINT,
            ThreadPool.Names.GENERIC,
            UpdateVisibleCheckpointRequest::new,
            new UpdateVisibleCheckpointRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_MERGED_SEGMENT_FILES,
            ThreadPool.Names.GENERIC,
            GetSegmentFilesRequest::new,
            new GetMergedSegmentFilesRequestHandler()
        );
    }

    public SegmentReplicationSourceService(
        IndicesService indicesService,
        TransportService transportService,
        RecoverySettings recoverySettings
    ) {
        this(indicesService, transportService, recoverySettings, new OngoingSegmentReplications(indicesService, recoverySettings));
    }

    private class CheckpointInfoRequestHandler implements TransportRequestHandler<CheckpointInfoRequest> {
        @Override
        public void messageReceived(CheckpointInfoRequest request, TransportChannel channel, Task task) throws Exception {
            final ReplicationTimer timer = new ReplicationTimer();
            timer.start();
            final RemoteSegmentFileChunkWriter segmentSegmentFileChunkWriter = getRemoteSegmentFileChunkWriter(
                SegmentReplicationTargetService.Actions.FILE_CHUNK,
                request,
                request.getCheckpoint().getShardId(),
                recoverySettings.internalActionRetryTimeout(),
                recoverySettings::replicationRateLimiter
            );
            final SegmentReplicationSourceHandler handler = ongoingSegmentReplications.prepareForReplication(
                request,
                segmentSegmentFileChunkWriter
            );
            channel.sendResponse(new CheckpointInfoResponse(handler.getCheckpoint(), handler.getInfosBytes()));
            timer.stop();
            logger.trace(
                new ParameterizedMessage(
                    "[replication id {}] Source node sent checkpoint info [{}] to target node [{}], timing: {}",
                    request.getReplicationId(),
                    handler.getCheckpoint(),
                    request.getTargetNode().getId(),
                    timer.time()
                )
            );
        }
    }

    private class GetSegmentFilesRequestHandler implements TransportRequestHandler<GetSegmentFilesRequest> {
        @Override
        public void messageReceived(GetSegmentFilesRequest request, TransportChannel channel, Task task) throws Exception {
            ongoingSegmentReplications.startSegmentCopy(request, new ChannelActionListener<>(channel, Actions.GET_SEGMENT_FILES, request));
        }
    }

    private class UpdateVisibleCheckpointRequestHandler implements TransportRequestHandler<UpdateVisibleCheckpointRequest> {
        @Override
        public void messageReceived(UpdateVisibleCheckpointRequest request, TransportChannel channel, Task task) throws Exception {
            try {
                IndexService indexService = indicesService.indexServiceSafe(request.getPrimaryShardId().getIndex());
                IndexShard indexShard = indexService.getShard(request.getPrimaryShardId().id());
                indexShard.updateVisibleCheckpointForShard(request.getTargetAllocationId(), request.getCheckpoint());
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }
    }

    private class GetMergedSegmentFilesRequestHandler implements TransportRequestHandler<GetSegmentFilesRequest> {
        @Override
        public void messageReceived(GetSegmentFilesRequest request, TransportChannel channel, Task task) throws Exception {
            ActionListener<GetSegmentFilesResponse> listener = new ChannelActionListener<>(
                channel,
                Actions.GET_MERGED_SEGMENT_FILES,
                request
            );
            final ShardId shardId = request.getCheckpoint().getShardId();
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            IndexShard indexShard = indexService.getShard(shardId.id());
            if (indexShard.state().equals(IndexShardState.STARTED) == false || indexShard.isPrimaryMode() == false) {
                throw new IllegalStateException(String.format(Locale.ROOT, "%s is not a started primary shard", shardId));
            }

            RemoteSegmentFileChunkWriter mergedSegmentFileChunkWriter = getRemoteSegmentFileChunkWriter(
                SegmentReplicationTargetService.Actions.MERGED_SEGMENT_FILE_CHUNK,
                request,
                request.getCheckpoint().getShardId(),
                recoverySettings.getMergedSegmentReplicationTimeout(),
                recoverySettings::mergedSegmentReplicationRateLimiter
            );

            SegmentFileTransferHandler mergedSegmentFileTransferHandler = new SegmentFileTransferHandler(
                indexShard,
                request.getTargetNode(),
                mergedSegmentFileChunkWriter,
                logger,
                indexShard.getThreadPool(),
                new CancellableThreads(),
                Math.toIntExact(indexShard.getRecoverySettings().getChunkSize().getBytes()),
                indexShard.getRecoverySettings().getMaxConcurrentFileChunks()
            );

            final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = mergedSegmentFileTransferHandler
                .createTransfer(
                    indexShard.store(),
                    request.getCheckpoint().getMetadataMap().values().toArray(new StoreFileMetadata[0]),
                    () -> 0,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            listener.onResponse(new GetSegmentFilesResponse(request.getFilesToFetch()));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                );
            transfer.start();
        }
    }

    private RemoteSegmentFileChunkWriter getRemoteSegmentFileChunkWriter(
        String action,
        SegmentReplicationTransportRequest request,
        ShardId shardId,
        TimeValue retryTimeout,
        Supplier<RateLimiter> rateLimiterSupplier
    ) {
        return new RemoteSegmentFileChunkWriter(
            request.getReplicationId(),
            recoverySettings,
            new RetryableTransportClient(transportService, request.getTargetNode(), retryTimeout, logger),
            shardId,
            action,
            new AtomicLong(0),
            (throttleTime) -> {},
            rateLimiterSupplier
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingSegmentReplications.cancelReplication(removedNode);
            }
        }
        // if a replica for one of the primary shards on this node has closed,
        // we need to ensure its state has cleared up in ongoing replications.
        if (event.routingTableChanged()) {
            for (IndexService indexService : indicesService) {
                if (indexService.getIndexSettings().isSegRepEnabledOrRemoteNode()) {
                    for (IndexShard indexShard : indexService) {
                        if (indexShard.routingEntry().primary()) {
                            final IndexMetadata indexMetadata = indexService.getIndexSettings().getIndexMetadata();
                            final Set<String> inSyncAllocationIds = new HashSet<>(
                                indexMetadata.inSyncAllocationIds(indexShard.shardId().id())
                            );
                            if (indexShard.isPrimaryMode()) {
                                final Set<String> shardTrackerInSyncIds = indexShard.getReplicationGroup().getInSyncAllocationIds();
                                inSyncAllocationIds.addAll(shardTrackerInSyncIds);
                            }
                            ongoingSegmentReplications.clearOutOfSyncIds(indexShard.shardId(), inSyncAllocationIds);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    /**
     *
     * Before a primary shard is closed, cancel any ongoing replications to release incref'd segments.
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null && indexShard.indexSettings().isSegRepEnabledOrRemoteNode()) {
            ongoingSegmentReplications.cancel(indexShard, "shard is closed");
        }
    }

    /**
     * Cancels any replications on this node to a replica that has been promoted as primary.
     */
    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        if (indexShard != null
            && indexShard.indexSettings().isSegRepEnabledOrRemoteNode()
            && oldRouting.primary() == false
            && newRouting.primary()) {
            ongoingSegmentReplications.cancel(indexShard.routingEntry().allocationId().getId(), "Relocating primary shard.");
        }
    }

}
