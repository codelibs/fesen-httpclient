/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationMode;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequest;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationResponse;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationTask;
import org.codelibs.fesen.opensearch.action.support.replication.TransportReplicationAction;
import org.codelibs.fesen.opensearch.cluster.action.shard.ShardStateAction;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.opensearch.common.util.concurrent.ThreadContextAccess;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.index.IndexNotFoundException;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.index.shard.IndexShardClosedException;
import org.codelibs.fesen.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationTimer;
import org.codelibs.fesen.opensearch.node.NodeClosedException;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportException;
import org.codelibs.fesen.opensearch.transport.TransportRequest;
import org.codelibs.fesen.opensearch.transport.TransportResponseHandler;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class for publish checkpoint.
 *
 * @opensearch.api
 */

public abstract class AbstractPublishCheckpointAction<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>> extends TransportReplicationAction<
        Request,
        ReplicaRequest,
        ReplicationResponse> {

    private final Logger logger;

    public AbstractPublishCheckpointAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String threadPoolName,
        Logger logger
    ) {
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            requestReader,
            replicaRequestReader,
            threadPoolName
        );
        this.logger = logger;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.indexSettings().isAssignedOnRemoteNode()) {
            return ReplicationMode.FULL_REPLICATION;
        }
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish checkpoint request to shard
     */
    final void doPublish(
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        TransportRequest request,
        String action,
        boolean waitForCompletion,
        TimeValue waitTimeout,
        ActionListener<Void> listener
    ) {
        ActionListener<Void> notifyOnceListener = ActionListener.notifyOnce(listener);
        String primaryAllocationId = indexShard.routingEntry().allocationId().getId();
        long primaryTerm = indexShard.getPendingPrimaryTerm();
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            ThreadContextAccess.doPrivilegedVoid(threadContext::markAsSystemContext);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", action, request);
            final ReplicationTimer timer = new ReplicationTimer();
            timer.start();
            CountDownLatch latch = new CountDownLatch(1);
            transportService.sendChildRequest(
                indexShard.recoveryState().getTargetNode(),
                transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        try {
                            timer.stop();
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "[shardId {}] Completed publishing checkpoint [{}], timing: {}",
                                    indexShard.shardId().getId(),
                                    checkpoint,
                                    timer.time()
                                )
                            );
                            task.setPhase("finished");
                            taskManager.unregister(task);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException e) {
                        try {
                            timer.stop();
                            logger.debug(
                                "[shardId {}] Failed to publish checkpoint [{}], timing: {}",
                                indexShard.shardId().getId(),
                                checkpoint,
                                timer.time()
                            );
                            task.setPhase("finished");
                            taskManager.unregister(task);
                            if (ExceptionsHelper.unwrap(
                                e,
                                NodeClosedException.class,
                                IndexNotFoundException.class,
                                AlreadyClosedException.class,
                                IndexShardClosedException.class,
                                ShardNotInPrimaryModeException.class
                            ) != null) {
                                // Node is shutting down or the index was deleted or the shard is closed
                                return;
                            }
                            logger.warn(
                                new ParameterizedMessage(
                                    "{} segment replication checkpoint [{}] publishing failed",
                                    indexShard.shardId(),
                                    checkpoint
                                ),
                                e
                            );
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            );
            logger.trace(
                () -> new ParameterizedMessage(
                    "[shardId {}] Publishing replication checkpoint [{}]",
                    checkpoint.getShardId().getId(),
                    checkpoint
                )
            );
            if (waitForCompletion) {
                try {
                    if (latch.await(waitTimeout.seconds(), TimeUnit.SECONDS) == false) {
                        notifyOnceListener.onFailure(
                            new TimeoutException("Timed out waiting for publish checkpoint to complete. Checkpoint: " + checkpoint)
                        );
                    }
                } catch (InterruptedException e) {
                    notifyOnceListener.onFailure(e);
                    logger.warn(
                        () -> new ParameterizedMessage("Interrupted while waiting for publish checkpoint complete [{}]", checkpoint),
                        e
                    );
                }
            }
            notifyOnceListener.onResponse(null);
        } catch (Exception e) {
            notifyOnceListener.onFailure(e);
        }
    }

    @Override
    final protected void shardOperationOnReplica(ReplicaRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        Objects.requireNonNull(shardRequest);
        Objects.requireNonNull(replica);
        ActionListener.completeWith(listener, () -> {
            logger.trace(() -> new ParameterizedMessage("Checkpoint {} received on replica {}", shardRequest, replica.shardId()));
            // Condition for ensuring that we ignore Segrep checkpoints received on Docrep shard copies.
            // This case will hit iff the replica hosting node is not remote enabled and replication type != SEGMENT
            if (replica.indexSettings().isAssignedOnRemoteNode() == false && replica.indexSettings().isSegRepLocalEnabled() == false) {
                logger.trace("Received segrep checkpoint on a docrep shard copy during an ongoing remote migration. NoOp.");
                return new ReplicaResult();
            }
            doReplicaOperation(shardRequest, replica);
            return new ReplicaResult();
        });
    }

    /**
     * Execute the specified replica operation.
     *
     * @param shardRequest the request to the replica shard
     * @param replica      the replica shard to perform the operation on
     */
    protected abstract void doReplicaOperation(ReplicaRequest shardRequest, IndexShard replica);
}
