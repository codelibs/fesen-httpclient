/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch;

import org.codelibs.fesen.opensearch.transport.client.transport.NoNodeAvailableException;

import static org.codelibs.fesen.opensearch.OpenSearchException.OpenSearchExceptionHandle;
import static org.codelibs.fesen.opensearch.OpenSearchException.OpenSearchExceptionHandleRegistry.registerExceptionHandle;
import static org.codelibs.fesen.opensearch.OpenSearchException.UNKNOWN_VERSION_ADDED;
import static org.codelibs.fesen.opensearch.Version.V_2_10_0;
import static org.codelibs.fesen.opensearch.Version.V_2_13_0;
import static org.codelibs.fesen.opensearch.Version.V_2_17_0;
import static org.codelibs.fesen.opensearch.Version.V_2_18_0;
import static org.codelibs.fesen.opensearch.Version.V_2_1_0;
import static org.codelibs.fesen.opensearch.Version.V_2_4_0;
import static org.codelibs.fesen.opensearch.Version.V_2_5_0;
import static org.codelibs.fesen.opensearch.Version.V_2_6_0;
import static org.codelibs.fesen.opensearch.Version.V_2_7_0;
import static org.codelibs.fesen.opensearch.Version.V_3_0_0;
import static org.codelibs.fesen.opensearch.Version.V_3_2_0;
import static org.codelibs.fesen.opensearch.Version.V_3_7_0;
import static org.codelibs.fesen.opensearch.Version.V_3_8_0;

/**
 * Utility class to register server exceptions
 *
 * @opensearch.internal
 */
public final class OpenSearchServerException {

    private OpenSearchServerException() {
        // no ctor:
    }

    /**
     * Setting a higher base exception id to avoid conflicts.
     */
    private static final int CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID = 10000;

    public static void registerExceptions() {
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.dfs.DfsPhaseExecutionException.class,
                org.codelibs.fesen.opensearch.search.dfs.DfsPhaseExecutionException::new,
                1,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.util.CancellableThreads.ExecutionCancelledException.class,
                org.codelibs.fesen.opensearch.common.util.CancellableThreads.ExecutionCancelledException::new,
                2,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.discovery.ClusterManagerNotDiscoveredException.class,
                org.codelibs.fesen.opensearch.discovery.ClusterManagerNotDiscoveredException::new,
                3,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.OpenSearchSecurityException.class,
                org.codelibs.fesen.opensearch.OpenSearchSecurityException::new,
                4,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.snapshots.IndexShardRestoreException.class,
                org.codelibs.fesen.opensearch.index.snapshots.IndexShardRestoreException::new,
                5,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.IndexClosedException.class,
                org.codelibs.fesen.opensearch.indices.IndexClosedException::new,
                6,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.http.BindHttpException.class,
                org.codelibs.fesen.opensearch.http.BindHttpException::new,
                7,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.search.ReduceSearchPhaseException.class,
                org.codelibs.fesen.opensearch.action.search.ReduceSearchPhaseException::new,
                8,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.node.NodeClosedException.class,
                org.codelibs.fesen.opensearch.node.NodeClosedException::new,
                9,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.SnapshotFailedEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.SnapshotFailedEngineException::new,
                10,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.ShardNotFoundException.class,
                org.codelibs.fesen.opensearch.index.shard.ShardNotFoundException::new,
                11,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.ConnectTransportException.class,
                org.codelibs.fesen.opensearch.transport.ConnectTransportException::new,
                12,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NotSerializableTransportException.class,
                org.codelibs.fesen.opensearch.transport.NotSerializableTransportException::new,
                13,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.ResponseHandlerFailureTransportException.class,
                org.codelibs.fesen.opensearch.transport.ResponseHandlerFailureTransportException::new,
                14,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.IndexCreationException.class,
                org.codelibs.fesen.opensearch.indices.IndexCreationException::new,
                15,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.IndexNotFoundException.class,
                org.codelibs.fesen.opensearch.index.IndexNotFoundException::new,
                16,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.routing.IllegalShardRoutingStateException.class,
                org.codelibs.fesen.opensearch.cluster.routing.IllegalShardRoutingStateException::new,
                17,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastShardOperationFailedException.class,
                org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastShardOperationFailedException::new,
                18,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.ResourceNotFoundException.class,
                org.codelibs.fesen.opensearch.ResourceNotFoundException::new,
                19,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.ActionTransportException.class,
                org.codelibs.fesen.opensearch.transport.ActionTransportException::new,
                20,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.OpenSearchGenerationException.class,
                org.codelibs.fesen.opensearch.OpenSearchGenerationException::new,
                21,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 22 was CreateFailedEngineException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardStartedException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardStartedException::new,
                23,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.SearchContextMissingException.class,
                org.codelibs.fesen.opensearch.search.SearchContextMissingException::new,
                24,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.script.GeneralScriptException.class,
                org.codelibs.fesen.opensearch.script.GeneralScriptException::new,
                25,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 26 was BatchOperationException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotCreationException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotCreationException::new,
                27,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.DocumentMissingException.class,
                org.codelibs.fesen.opensearch.index.engine.DocumentMissingException::new,
                29,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotException::new,
                30,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.InvalidAliasNameException.class,
                org.codelibs.fesen.opensearch.indices.InvalidAliasNameException::new,
                31,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.InvalidIndexNameException.class,
                org.codelibs.fesen.opensearch.indices.InvalidIndexNameException::new,
                32,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.IndexPrimaryShardNotAllocatedException.class,
                org.codelibs.fesen.opensearch.indices.IndexPrimaryShardNotAllocatedException::new,
                33,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.TransportException.class,
                org.codelibs.fesen.opensearch.transport.TransportException::new,
                34,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.SearchException.class,
                org.codelibs.fesen.opensearch.search.SearchException::new,
                36,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.mapper.MapperException.class,
                org.codelibs.fesen.opensearch.index.mapper.MapperException::new,
                37,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.InvalidTypeNameException.class,
                org.codelibs.fesen.opensearch.indices.InvalidTypeNameException::new,
                38,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotRestoreException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotRestoreException::new,
                39,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardClosedException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardClosedException::new,
                41,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.recovery.RecoverFilesRecoveryException.class,
                org.codelibs.fesen.opensearch.indices.recovery.RecoverFilesRecoveryException::new,
                42,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.translog.TruncatedTranslogException.class,
                org.codelibs.fesen.opensearch.index.translog.TruncatedTranslogException::new,
                43,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.recovery.RecoveryFailedException.class,
                org.codelibs.fesen.opensearch.indices.recovery.RecoveryFailedException::new,
                44,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardRelocatedException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardRelocatedException::new,
                45,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NodeShouldNotConnectException.class,
                org.codelibs.fesen.opensearch.transport.NodeShouldNotConnectException::new,
                46,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.translog.TranslogCorruptedException.class,
                org.codelibs.fesen.opensearch.index.translog.TranslogCorruptedException::new,
                48,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException.class,
                org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException::new,
                49,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.fetch.FetchPhaseExecutionException.class,
                org.codelibs.fesen.opensearch.search.fetch.FetchPhaseExecutionException::new,
                50,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException::new,
                52,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.EngineException.class,
                org.codelibs.fesen.opensearch.index.engine.EngineException::new,
                53,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.NoSuchNodeException.class,
                org.codelibs.fesen.opensearch.action.NoSuchNodeException::new,
                55,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.settings.SettingsException.class,
                org.codelibs.fesen.opensearch.common.settings.SettingsException::new,
                56,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.IndexTemplateMissingException.class,
                org.codelibs.fesen.opensearch.indices.IndexTemplateMissingException::new,
                57,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.SendRequestTransportException.class,
                org.codelibs.fesen.opensearch.transport.SendRequestTransportException::new,
                58,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 59 used to be OpenSearchRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.AliasFilterParsingException.class,
                org.codelibs.fesen.opensearch.indices.AliasFilterParsingException::new,
                63,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.gateway.GatewayException.class,
                org.codelibs.fesen.opensearch.gateway.GatewayException::new,
                65,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardNotRecoveringException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardNotRecoveringException::new,
                66,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.http.HttpException.class,
                org.codelibs.fesen.opensearch.http.HttpException::new,
                67,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.OpenSearchException.class,
                org.codelibs.fesen.opensearch.OpenSearchException::new,
                68,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotMissingException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotMissingException::new,
                69,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.PrimaryMissingActionException.class,
                org.codelibs.fesen.opensearch.action.PrimaryMissingActionException::new,
                70,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.SearchParseException.class,
                org.codelibs.fesen.opensearch.search.SearchParseException::new,
                72,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.FailedNodeException.class,
                org.codelibs.fesen.opensearch.action.FailedNodeException::new,
                71,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.ConcurrentSnapshotExecutionException.class,
                org.codelibs.fesen.opensearch.snapshots.ConcurrentSnapshotExecutionException::new,
                73,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.blobstore.BlobStoreException.class,
                org.codelibs.fesen.opensearch.common.blobstore.BlobStoreException::new,
                74,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.IncompatibleClusterStateVersionException.class,
                org.codelibs.fesen.opensearch.cluster.IncompatibleClusterStateVersionException::new,
                75,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.RecoveryEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.RecoveryEngineException::new,
                76,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.util.concurrent.UncategorizedExecutionException.class,
                org.codelibs.fesen.opensearch.common.util.concurrent.UncategorizedExecutionException::new,
                77,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.TimestampParsingException.class,
                org.codelibs.fesen.opensearch.action.TimestampParsingException::new,
                78,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.RoutingMissingException.class,
                org.codelibs.fesen.opensearch.action.RoutingMissingException::new,
                79,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.snapshots.IndexShardRestoreFailedException.class,
                org.codelibs.fesen.opensearch.index.snapshots.IndexShardRestoreFailedException::new,
                81,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.repositories.RepositoryException.class,
                org.codelibs.fesen.opensearch.repositories.RepositoryException::new,
                82,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.ReceiveTimeoutTransportException.class,
                org.codelibs.fesen.opensearch.transport.ReceiveTimeoutTransportException::new,
                83,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NodeDisconnectedException.class,
                org.codelibs.fesen.opensearch.transport.NodeDisconnectedException::new,
                84,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 85 used to be for AlreadyExpiredException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.aggregations.AggregationExecutionException.class,
                org.codelibs.fesen.opensearch.search.aggregations.AggregationExecutionException::new,
                86,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 87 used to be for MergeMappingException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.InvalidIndexTemplateException.class,
                org.codelibs.fesen.opensearch.indices.InvalidIndexTemplateException::new,
                88,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.RefreshFailedEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.RefreshFailedEngineException::new,
                90,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.aggregations.AggregationInitializationException.class,
                org.codelibs.fesen.opensearch.search.aggregations.AggregationInitializationException::new,
                91,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.recovery.DelayRecoveryException.class,
                org.codelibs.fesen.opensearch.indices.recovery.DelayRecoveryException::new,
                92,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 93 used to be for IndexWarmerMissingException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(NoNodeAvailableException.class, NoNodeAvailableException::new, 94, UNKNOWN_VERSION_ADDED)
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.InvalidSnapshotNameException.class,
                org.codelibs.fesen.opensearch.snapshots.InvalidSnapshotNameException::new,
                96,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IllegalIndexShardStateException.class,
                org.codelibs.fesen.opensearch.index.shard.IllegalIndexShardStateException::new,
                97,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.core.index.snapshots.IndexShardSnapshotException.class,
                org.codelibs.fesen.opensearch.core.index.snapshots.IndexShardSnapshotException::new,
                98,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardNotStartedException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardNotStartedException::new,
                99,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.search.SearchPhaseExecutionException.class,
                org.codelibs.fesen.opensearch.action.search.SearchPhaseExecutionException::new,
                100,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.ActionNotFoundTransportException.class,
                org.codelibs.fesen.opensearch.transport.ActionNotFoundTransportException::new,
                101,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.TransportSerializationException.class,
                org.codelibs.fesen.opensearch.transport.TransportSerializationException::new,
                102,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.RemoteTransportException.class,
                org.codelibs.fesen.opensearch.transport.RemoteTransportException::new,
                103,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.EngineCreationFailureException.class,
                org.codelibs.fesen.opensearch.index.engine.EngineCreationFailureException::new,
                104,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.routing.RoutingException.class,
                org.codelibs.fesen.opensearch.cluster.routing.RoutingException::new,
                105,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardRecoveryException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardRecoveryException::new,
                106,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.repositories.RepositoryMissingException.class,
                org.codelibs.fesen.opensearch.repositories.RepositoryMissingException::new,
                107,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.DocumentSourceMissingException.class,
                org.codelibs.fesen.opensearch.index.engine.DocumentSourceMissingException::new,
                109,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 110 used to be FlushNotAllowedEngineException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.settings.NoClassSettingsException.class,
                org.codelibs.fesen.opensearch.common.settings.NoClassSettingsException::new,
                111,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.BindTransportException.class,
                org.codelibs.fesen.opensearch.transport.BindTransportException::new,
                112,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.admin.indices.alias.AliasesNotFoundException.class,
                org.codelibs.fesen.opensearch.action.admin.indices.alias.AliasesNotFoundException::new,
                113,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.IndexShardRecoveringException.class,
                org.codelibs.fesen.opensearch.index.shard.IndexShardRecoveringException::new,
                114,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.translog.TranslogException.class,
                org.codelibs.fesen.opensearch.index.translog.TranslogException::new,
                115,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.metadata.ProcessClusterEventTimeoutException.class,
                org.codelibs.fesen.opensearch.cluster.metadata.ProcessClusterEventTimeoutException::new,
                116,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.support.replication.ReplicationOperation.RetryOnPrimaryException.class,
                org.codelibs.fesen.opensearch.action.support.replication.ReplicationOperation.RetryOnPrimaryException::new,
                117,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.OpenSearchTimeoutException.class,
                org.codelibs.fesen.opensearch.OpenSearchTimeoutException::new,
                118,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.query.QueryPhaseExecutionException.class,
                org.codelibs.fesen.opensearch.search.query.QueryPhaseExecutionException::new,
                119,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.repositories.RepositoryVerificationException.class,
                org.codelibs.fesen.opensearch.repositories.RepositoryVerificationException::new,
                120,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.aggregations.InvalidAggregationPathException.class,
                org.codelibs.fesen.opensearch.search.aggregations.InvalidAggregationPathException::new,
                121,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 123 used to be IndexAlreadyExistsException and was renamed
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                ResourceAlreadyExistsException.class,
                ResourceAlreadyExistsException::new,
                123,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 124 used to be Script.ScriptParseException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.TcpTransport.HttpRequestOnTransportException.class,
                org.codelibs.fesen.opensearch.transport.TcpTransport.HttpRequestOnTransportException::new,
                125,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.mapper.MapperParsingException.class,
                org.codelibs.fesen.opensearch.index.mapper.MapperParsingException::new,
                126,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 127 used to be org.codelibs.fesen.opensearch.search.SearchContextException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilderException.class,
                org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilderException::new,
                128,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 129 was EngineClosedException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.NoShardAvailableActionException.class,
                org.codelibs.fesen.opensearch.action.NoShardAvailableActionException::new,
                130,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.UnavailableShardsException.class,
                org.codelibs.fesen.opensearch.action.UnavailableShardsException::new,
                131,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.FlushFailedEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.FlushFailedEngineException::new,
                132,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NodeNotConnectedException.class,
                org.codelibs.fesen.opensearch.transport.NodeNotConnectedException::new,
                134,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.mapper.StrictDynamicMappingException.class,
                org.codelibs.fesen.opensearch.index.mapper.StrictDynamicMappingException::new,
                135,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
                org.codelibs.fesen.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new,
                136,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.TypeMissingException.class,
                org.codelibs.fesen.opensearch.indices.TypeMissingException::new,
                137,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.coordination.FailedToCommitClusterStateException.class,
                org.codelibs.fesen.opensearch.cluster.coordination.FailedToCommitClusterStateException::new,
                140,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.query.QueryShardException.class,
                org.codelibs.fesen.opensearch.index.query.QueryShardException::new,
                141,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.action.shard.ShardStateAction.NoLongerPrimaryShardException.class,
                org.codelibs.fesen.opensearch.cluster.action.shard.ShardStateAction.NoLongerPrimaryShardException::new,
                142,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.script.ScriptException.class,
                org.codelibs.fesen.opensearch.script.ScriptException::new,
                143,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.NotClusterManagerException.class,
                org.codelibs.fesen.opensearch.cluster.NotClusterManagerException::new,
                144,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.OpenSearchStatusException.class,
                org.codelibs.fesen.opensearch.OpenSearchStatusException::new,
                145,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.env.ShardLockObtainFailedException.class,
                org.codelibs.fesen.opensearch.env.ShardLockObtainFailedException::new,
                147,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 148 was UnknownNamedObjectException
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.aggregations.MultiBucketConsumerService.TooManyBucketsException.class,
                org.codelibs.fesen.opensearch.search.aggregations.MultiBucketConsumerService.TooManyBucketsException::new,
                149,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.coordination.CoordinationStateRejectedException.class,
                org.codelibs.fesen.opensearch.cluster.coordination.CoordinationStateRejectedException::new,
                150,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotInProgressException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotInProgressException::new,
                151,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NoSuchRemoteClusterException.class,
                org.codelibs.fesen.opensearch.transport.NoSuchRemoteClusterException::new,
                152,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseAlreadyExistsException.class,
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseAlreadyExistsException::new,
                153,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseNotFoundException.class,
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseNotFoundException::new,
                154,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.ShardNotInPrimaryModeException.class,
                org.codelibs.fesen.opensearch.index.shard.ShardNotInPrimaryModeException::new,
                155,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
                org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
                156,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.ingest.IngestProcessorException.class,
                org.codelibs.fesen.opensearch.ingest.IngestProcessorException::new,
                157,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.recovery.PeerRecoveryNotFound.class,
                org.codelibs.fesen.opensearch.indices.recovery.PeerRecoveryNotFound::new,
                158,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.coordination.NodeHealthCheckFailureException.class,
                org.codelibs.fesen.opensearch.cluster.coordination.NodeHealthCheckFailureException::new,
                159,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.NoSeedNodeLeftException.class,
                org.codelibs.fesen.opensearch.transport.NoSeedNodeLeftException::new,
                160,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.replication.common.ReplicationFailedException.class,
                org.codelibs.fesen.opensearch.indices.replication.common.ReplicationFailedException::new,
                161,
                V_2_1_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.shard.PrimaryShardClosedException.class,
                org.codelibs.fesen.opensearch.index.shard.PrimaryShardClosedException::new,
                162,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.decommission.DecommissioningFailedException.class,
                org.codelibs.fesen.opensearch.cluster.decommission.DecommissioningFailedException::new,
                163,
                V_2_4_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.decommission.NodeDecommissionedException.class,
                org.codelibs.fesen.opensearch.cluster.decommission.NodeDecommissionedException::new,
                164,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.service.ClusterManagerThrottlingException.class,
                org.codelibs.fesen.opensearch.cluster.service.ClusterManagerThrottlingException::new,
                165,
                Version.V_2_5_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.snapshots.SnapshotInUseDeletionException.class,
                org.codelibs.fesen.opensearch.snapshots.SnapshotInUseDeletionException::new,
                166,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.routing.UnsupportedWeightedRoutingStateException.class,
                org.codelibs.fesen.opensearch.cluster.routing.UnsupportedWeightedRoutingStateException::new,
                167,
                V_2_5_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.routing.PreferenceBasedSearchNotAllowedException.class,
                org.codelibs.fesen.opensearch.cluster.routing.PreferenceBasedSearchNotAllowedException::new,
                168,
                V_2_6_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.routing.NodeWeighedAwayException.class,
                org.codelibs.fesen.opensearch.cluster.routing.NodeWeighedAwayException::new,
                169,
                V_2_6_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineProcessingException.class,
                org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineProcessingException::new,
                170,
                V_2_7_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.crypto.CryptoRegistryException.class,
                org.codelibs.fesen.opensearch.crypto.CryptoRegistryException::new,
                171,
                V_2_10_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.admin.indices.view.ViewNotFoundException.class,
                org.codelibs.fesen.opensearch.action.admin.indices.view.ViewNotFoundException::new,
                172,
                V_2_13_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.action.admin.indices.view.ViewAlreadyExistsException.class,
                org.codelibs.fesen.opensearch.action.admin.indices.view.ViewAlreadyExistsException::new,
                173,
                V_2_13_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.indices.InvalidIndexContextException.class,
                org.codelibs.fesen.opensearch.indices.InvalidIndexContextException::new,
                174,
                V_2_17_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.common.breaker.ResponseLimitBreachedException.class,
                org.codelibs.fesen.opensearch.common.breaker.ResponseLimitBreachedException::new,
                175,
                V_2_18_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.cluster.block.IndexCreateBlockException.class,
                org.codelibs.fesen.opensearch.cluster.block.IndexCreateBlockException::new,
                CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID + 1,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.IngestionEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.IngestionEngineException::new,
                176,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.transport.stream.StreamException.class,
                org.codelibs.fesen.opensearch.transport.stream.StreamException::new,
                177,
                V_3_2_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.index.engine.dataformat.merge.MergeFailedEngineException.class,
                org.codelibs.fesen.opensearch.index.engine.dataformat.merge.MergeFailedEngineException::new,
                178,
                V_3_7_0
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.codelibs.fesen.opensearch.storage.action.tiering.MergeDrainTimeoutException.class,
                org.codelibs.fesen.opensearch.storage.action.tiering.MergeDrainTimeoutException::new,
                179,
                V_3_8_0
            )
        );
    }
}
