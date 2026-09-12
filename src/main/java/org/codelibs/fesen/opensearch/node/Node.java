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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.codelibs.fesen.opensearch.Build;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.OpenSearchTimeoutException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionModule;
import org.codelibs.fesen.opensearch.action.ActionModule.DynamicActionRegistry;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.codelibs.fesen.opensearch.action.admin.indices.view.ViewService;
import org.codelibs.fesen.opensearch.action.search.SearchExecutionStatsCollector;
import org.codelibs.fesen.opensearch.action.search.SearchPhaseController;
import org.codelibs.fesen.opensearch.action.search.SearchRequestOperationsCompositeListenerFactory;
import org.codelibs.fesen.opensearch.action.search.SearchRequestOperationsListener;
import org.codelibs.fesen.opensearch.action.search.SearchRequestSlowLog;
import org.codelibs.fesen.opensearch.action.search.SearchRequestStats;
import org.codelibs.fesen.opensearch.action.search.SearchTaskRequestOperationsListener;
import org.codelibs.fesen.opensearch.action.search.SearchTransportService;
import org.codelibs.fesen.opensearch.action.search.StreamSearchTransportService;
import org.codelibs.fesen.opensearch.action.support.TransportAction;
import org.codelibs.fesen.opensearch.action.update.UpdateHelper;
import org.codelibs.fesen.opensearch.arrow.spi.NativeAllocator;
import org.codelibs.fesen.opensearch.arrow.spi.PoolGroup;
import org.codelibs.fesen.opensearch.bootstrap.BootstrapCheck;
import org.codelibs.fesen.opensearch.bootstrap.BootstrapContext;
import org.codelibs.fesen.opensearch.bootstrap.BootstrapSettings;
import org.codelibs.fesen.opensearch.cluster.ClusterInfoService;
import org.codelibs.fesen.opensearch.cluster.ClusterManagerMetrics;
import org.codelibs.fesen.opensearch.cluster.ClusterModule;
import org.codelibs.fesen.opensearch.cluster.ClusterName;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.ClusterStateObserver;
import org.codelibs.fesen.opensearch.cluster.InternalClusterInfoService;
import org.codelibs.fesen.opensearch.cluster.NodeConnectionsService;
import org.codelibs.fesen.opensearch.cluster.StreamNodeConnectionsService;
import org.codelibs.fesen.opensearch.cluster.action.index.MappingUpdatedAction;
import org.codelibs.fesen.opensearch.cluster.action.shard.LocalShardStateAction;
import org.codelibs.fesen.opensearch.cluster.action.shard.ShardStateAction;
import org.codelibs.fesen.opensearch.cluster.applicationtemplates.SystemTemplatesPlugin;
import org.codelibs.fesen.opensearch.cluster.applicationtemplates.SystemTemplatesService;
import org.codelibs.fesen.opensearch.cluster.coordination.PersistedStateRegistry;
import org.codelibs.fesen.opensearch.cluster.metadata.AliasValidator;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataCreateDataStreamService;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataDataStreamsService;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.codelibs.fesen.opensearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.codelibs.fesen.opensearch.cluster.metadata.TemplateUpgradeService;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodeRole;
import org.codelibs.fesen.opensearch.cluster.routing.BatchedRerouteService;
import org.codelibs.fesen.opensearch.cluster.routing.RerouteService;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.cluster.service.LocalClusterService;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.SetOnce;
import org.codelibs.fesen.opensearch.common.StopWatch;
import org.codelibs.fesen.opensearch.common.UUIDs;
import org.codelibs.fesen.opensearch.common.cache.module.CacheModule;
import org.codelibs.fesen.opensearch.common.cache.service.CacheService;
import org.codelibs.fesen.opensearch.common.lease.Releasables;
import org.codelibs.fesen.opensearch.common.lifecycle.Lifecycle;
import org.codelibs.fesen.opensearch.common.lifecycle.LifecycleComponent;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.logging.HeaderWarning;
import org.codelibs.fesen.opensearch.common.logging.NodeAndClusterIdStateListener;
import org.codelibs.fesen.opensearch.common.network.NetworkAddress;
import org.codelibs.fesen.opensearch.common.network.NetworkModule;
import org.codelibs.fesen.opensearch.common.network.NetworkService;
import org.codelibs.fesen.opensearch.common.settings.ClusterSettings;
import org.codelibs.fesen.opensearch.common.settings.ConsistentSettingsService;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.SettingUpgrader;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.settings.SettingsModule;
import org.codelibs.fesen.opensearch.common.unit.RatioValue;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.util.BigArrays;
import org.codelibs.fesen.opensearch.common.util.FeatureFlags;
import org.codelibs.fesen.opensearch.common.util.PageCacheRecycler;
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;
import org.codelibs.fesen.opensearch.core.Assertions;
import org.codelibs.fesen.opensearch.core.common.breaker.CircuitBreaker;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.opensearch.core.common.transport.BoundTransportAddress;
import org.codelibs.fesen.opensearch.core.common.transport.TransportAddress;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.indices.breaker.CircuitBreakerService;
import org.codelibs.fesen.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.crypto.CryptoHandlerRegistry;
import org.codelibs.fesen.opensearch.discovery.Discovery;
import org.codelibs.fesen.opensearch.discovery.DiscoveryModule;
import org.codelibs.fesen.opensearch.discovery.LocalDiscovery;
import org.codelibs.fesen.opensearch.env.Environment;
import org.codelibs.fesen.opensearch.env.NodeEnvironment;
import org.codelibs.fesen.opensearch.env.NodeMetadata;
import org.codelibs.fesen.opensearch.gateway.GatewayAllocator;
import org.codelibs.fesen.opensearch.gateway.GatewayMetaState;
import org.codelibs.fesen.opensearch.gateway.GatewayModule;
import org.codelibs.fesen.opensearch.gateway.GatewayService;
import org.codelibs.fesen.opensearch.gateway.MetaStateService;
import org.codelibs.fesen.opensearch.gateway.PersistedClusterStateService;
import org.codelibs.fesen.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateCleanupManager;
import org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateService;
import org.codelibs.fesen.opensearch.http.HttpServerTransport;
import org.codelibs.fesen.opensearch.identity.IdentityService;
import org.codelibs.fesen.opensearch.index.IndexModule;
import org.codelibs.fesen.opensearch.index.IndexSettings;
import org.codelibs.fesen.opensearch.index.IndexingPressureService;
import org.codelibs.fesen.opensearch.index.IngestionConsumerFactory;
import org.codelibs.fesen.opensearch.index.SegmentReplicationStatsTracker;
import org.codelibs.fesen.opensearch.index.analysis.AnalysisRegistry;
import org.codelibs.fesen.opensearch.index.autoforcemerge.AutoForceMergeManager;
import org.codelibs.fesen.opensearch.index.autoforcemerge.AutoForceMergeMetrics;
import org.codelibs.fesen.opensearch.index.compositeindex.CompositeIndexSettings;
import org.codelibs.fesen.opensearch.index.engine.EngineFactory;
import org.codelibs.fesen.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.codelibs.fesen.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.codelibs.fesen.opensearch.index.mapper.MappingTransformerRegistry;
import org.codelibs.fesen.opensearch.index.recovery.RemoteStoreRestoreService;
import org.codelibs.fesen.opensearch.index.remote.RemoteIndexPathUploader;
import org.codelibs.fesen.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.codelibs.fesen.opensearch.index.store.DefaultCompositeDirectoryFactory;
import org.codelibs.fesen.opensearch.index.store.DefaultDataFormatAwareStoreDirectoryFactory;
import org.codelibs.fesen.opensearch.index.store.IndexStoreListener;
import org.codelibs.fesen.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.FileCache;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.NodeCacheService;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.NodeCacheServiceCleaner;
import org.codelibs.fesen.opensearch.indices.IndicesModule;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.indices.RemoteStoreSettings;
import org.codelibs.fesen.opensearch.indices.ShardLimitValidator;
import org.codelibs.fesen.opensearch.indices.SystemIndexDescriptor;
import org.codelibs.fesen.opensearch.indices.SystemIndices;
import org.codelibs.fesen.opensearch.indices.analysis.AnalysisModule;
import org.codelibs.fesen.opensearch.indices.breaker.BreakerSettings;
import org.codelibs.fesen.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.codelibs.fesen.opensearch.indices.cluster.IndicesClusterStateService;
import org.codelibs.fesen.opensearch.indices.recovery.PeerRecoverySourceService;
import org.codelibs.fesen.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.codelibs.fesen.opensearch.indices.recovery.RecoverySettings;
import org.codelibs.fesen.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.codelibs.fesen.opensearch.indices.replication.SegmentReplicationSourceService;
import org.codelibs.fesen.opensearch.indices.replication.SegmentReplicationTargetService;
import org.codelibs.fesen.opensearch.indices.replication.SegmentReplicator;
import org.codelibs.fesen.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.codelibs.fesen.opensearch.indices.replication.checkpoint.PublishMergedSegmentAction;
import org.codelibs.fesen.opensearch.indices.replication.checkpoint.RemoteStorePublishMergedSegmentAction;
import org.codelibs.fesen.opensearch.indices.store.IndicesStore;
import org.codelibs.fesen.opensearch.ingest.IngestService;
import org.codelibs.fesen.opensearch.ingest.SystemIngestPipelineCache;
import org.codelibs.fesen.opensearch.monitor.MonitorService;
import org.codelibs.fesen.opensearch.monitor.NodeRuntimeMetrics;
import org.codelibs.fesen.opensearch.monitor.fs.FsHealthService;
import org.codelibs.fesen.opensearch.monitor.fs.FsServiceProvider;
import org.codelibs.fesen.opensearch.monitor.jvm.JvmInfo;
import org.codelibs.fesen.opensearch.monitor.os.OsProbe;
import org.codelibs.fesen.opensearch.monitor.process.ProcessProbe;
import org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeService;
import org.codelibs.fesen.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.codelibs.fesen.opensearch.node.resource.tracker.NodeResourceUsageTracker;
import org.codelibs.fesen.opensearch.persistent.PersistentTasksClusterService;
import org.codelibs.fesen.opensearch.persistent.PersistentTasksExecutor;
import org.codelibs.fesen.opensearch.persistent.PersistentTasksExecutorRegistry;
import org.codelibs.fesen.opensearch.persistent.PersistentTasksService;
import org.codelibs.fesen.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.codelibs.fesen.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.codelibs.fesen.opensearch.plugin.stats.NativeAllocatorStatsRegistry;
import org.codelibs.fesen.opensearch.plugins.ActionPlugin;
import org.codelibs.fesen.opensearch.plugins.AnalysisPlugin;
import org.codelibs.fesen.opensearch.plugins.BlockCacheRegistry;
import org.codelibs.fesen.opensearch.plugins.CachePlugin;
import org.codelibs.fesen.opensearch.plugins.CircuitBreakerPlugin;
import org.codelibs.fesen.opensearch.plugins.ClusterPlugin;
import org.codelibs.fesen.opensearch.plugins.CryptoKeyProviderPlugin;
import org.codelibs.fesen.opensearch.plugins.CryptoPlugin;
import org.codelibs.fesen.opensearch.plugins.DefaultPluginComponentRegistry;
import org.codelibs.fesen.opensearch.plugins.DiscoveryPlugin;
import org.codelibs.fesen.opensearch.plugins.EnginePlugin;
import org.codelibs.fesen.opensearch.plugins.ExtensionAwarePlugin;
import org.codelibs.fesen.opensearch.plugins.IdentityAwarePlugin;
import org.codelibs.fesen.opensearch.plugins.IdentityPlugin;
import org.codelibs.fesen.opensearch.plugins.IndexStorePlugin;
import org.codelibs.fesen.opensearch.plugins.IngestPlugin;
import org.codelibs.fesen.opensearch.plugins.IngestionConsumerPlugin;
import org.codelibs.fesen.opensearch.plugins.MapperPlugin;
import org.codelibs.fesen.opensearch.plugins.MetadataUpgrader;
import org.codelibs.fesen.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.codelibs.fesen.opensearch.plugins.NetworkPlugin;
import org.codelibs.fesen.opensearch.plugins.PersistentTaskPlugin;
import org.codelibs.fesen.opensearch.plugins.Plugin;
import org.codelibs.fesen.opensearch.plugins.PluginInfo;
import org.codelibs.fesen.opensearch.plugins.PluginsService;
import org.codelibs.fesen.opensearch.plugins.RepositoryPlugin;
import org.codelibs.fesen.opensearch.plugins.ScriptPlugin;
import org.codelibs.fesen.opensearch.plugins.SearchBackEndPlugin;
import org.codelibs.fesen.opensearch.plugins.SearchPipelinePlugin;
import org.codelibs.fesen.opensearch.plugins.SearchPlugin;
import org.codelibs.fesen.opensearch.plugins.SearchStatsContributor;
import org.codelibs.fesen.opensearch.plugins.SecureSettingsFactory;
import org.codelibs.fesen.opensearch.plugins.SystemIndexPlugin;
import org.codelibs.fesen.opensearch.plugins.TaskManagerClientPlugin;
import org.codelibs.fesen.opensearch.plugins.TelemetryAwarePlugin;
import org.codelibs.fesen.opensearch.plugins.TelemetryPlugin;
import org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.transport.AdmissionControlTransportInterceptor;
import org.codelibs.fesen.opensearch.repositories.RepositoriesModule;
import org.codelibs.fesen.opensearch.repositories.RepositoriesService;
import org.codelibs.fesen.opensearch.rest.RestController;
import org.codelibs.fesen.opensearch.script.ScriptContext;
import org.codelibs.fesen.opensearch.script.ScriptEngine;
import org.codelibs.fesen.opensearch.script.ScriptModule;
import org.codelibs.fesen.opensearch.script.ScriptService;
import org.codelibs.fesen.opensearch.search.SearchModule;
import org.codelibs.fesen.opensearch.search.SearchService;
import org.codelibs.fesen.opensearch.search.aggregations.support.AggregationUsageService;
import org.codelibs.fesen.opensearch.search.backpressure.SearchBackpressureService;
import org.codelibs.fesen.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.codelibs.fesen.opensearch.search.deciders.ConcurrentSearchRequestDecider;
import org.codelibs.fesen.opensearch.search.fetch.FetchPhase;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineService;
import org.codelibs.fesen.opensearch.search.query.QueryPhase;
import org.codelibs.fesen.opensearch.snapshots.InternalSnapshotsInfoService;
import org.codelibs.fesen.opensearch.snapshots.RestoreService;
import org.codelibs.fesen.opensearch.snapshots.SnapshotShardsService;
import org.codelibs.fesen.opensearch.snapshots.SnapshotsInfoService;
import org.codelibs.fesen.opensearch.snapshots.SnapshotsService;
import org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils;
import org.codelibs.fesen.opensearch.storage.directory.TieredDataFormatAwareStoreDirectoryFactory;
import org.codelibs.fesen.opensearch.storage.directory.TieredDirectoryFactory;
import org.codelibs.fesen.opensearch.storage.metrics.TierActionMetrics;
import org.codelibs.fesen.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.codelibs.fesen.opensearch.storage.tiering.HotToWarmTieringService;
import org.codelibs.fesen.opensearch.storage.tiering.WarmToHotTieringService;
import org.codelibs.fesen.opensearch.task.commons.clients.TaskManagerClient;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.tasks.TaskCancellationMonitoringService;
import org.codelibs.fesen.opensearch.tasks.TaskCancellationMonitoringSettings;
import org.codelibs.fesen.opensearch.tasks.TaskCancellationService;
import org.codelibs.fesen.opensearch.tasks.TaskResourceTrackingService;
import org.codelibs.fesen.opensearch.tasks.TaskResultsService;
import org.codelibs.fesen.opensearch.tasks.consumer.TopNSearchTasksLogger;
import org.codelibs.fesen.opensearch.telemetry.TelemetryModule;
import org.codelibs.fesen.opensearch.telemetry.TelemetrySettings;
import org.codelibs.fesen.opensearch.telemetry.metrics.MetricsRegistry;
import org.codelibs.fesen.opensearch.telemetry.metrics.MetricsRegistryFactory;
import org.codelibs.fesen.opensearch.telemetry.metrics.NoopMetricsRegistryFactory;
import org.codelibs.fesen.opensearch.telemetry.tracing.NoopTracerFactory;
import org.codelibs.fesen.opensearch.telemetry.tracing.Tracer;
import org.codelibs.fesen.opensearch.telemetry.tracing.TracerFactory;
import org.codelibs.fesen.opensearch.threadpool.ExecutorBuilder;
import org.codelibs.fesen.opensearch.threadpool.RunnableTaskExecutionListener;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.AuxTransport;
import org.codelibs.fesen.opensearch.transport.RemoteClusterService;
import org.codelibs.fesen.opensearch.transport.StreamTransportService;
import org.codelibs.fesen.opensearch.transport.Transport;
import org.codelibs.fesen.opensearch.transport.TransportInterceptor;
import org.codelibs.fesen.opensearch.transport.TransportService;
import org.codelibs.fesen.opensearch.transport.client.Client;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;
import org.codelibs.fesen.opensearch.usage.UsageService;
import org.codelibs.fesen.opensearch.watcher.ResourceWatcherService;
import org.codelibs.fesen.opensearch.wlm.WorkloadGroupService;
import org.codelibs.fesen.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.codelibs.fesen.opensearch.wlm.WorkloadManagementSettings;
import org.codelibs.fesen.opensearch.wlm.WorkloadManagementTransportInterceptor;
import org.codelibs.fesen.opensearch.wlm.cancellation.MaximumResourceTaskSelectionStrategy;
import org.codelibs.fesen.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.codelibs.fesen.opensearch.wlm.listeners.WorkloadGroupRequestOperationListener;
import org.codelibs.fesen.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import javax.net.ssl.SNIHostName;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.codelibs.fesen.opensearch.common.util.FeatureFlags.BACKGROUND_TASK_EXECUTION_EXPERIMENTAL;
import static org.codelibs.fesen.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.codelibs.fesen.opensearch.common.util.FeatureFlags.TELEMETRY;
import static org.codelibs.fesen.opensearch.index.ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY;
import static org.codelibs.fesen.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteClusterStateConfigured;
import static org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteDataAttributePresent;
import static org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreAttributePresent;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 *
 * @opensearch.internal
 */
public class Node {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING = Setting.boolSetting("node.portsfile", false, Property.NodeScope);

    /**
     * controls whether the node is allowed to persist things like metadata to disk
     * Note that this does not control whether the node stores actual indices (see
     * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
     * and {@link #NODE_MASTER_SETTING} must also be false.
     */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting(
        "node.local_storage",
        true,
        Property.Deprecated,
        Property.NodeScope
    );
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " + "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                }
            }
            return value;
        }, Property.NodeScope)
    );
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch (s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    private static final String ZERO = "0";

    public static final Setting<String> NODE_SEARCH_CACHE_SIZE_SETTING = new Setting<>(
        "node.search.cache.size",
        s -> (DiscoveryNode.isDedicatedWarmNode(s)) ? "80%" : ZERO,
        Node::validateFileCacheSize,
        Property.NodeScope
    );

    /**
     * The discovery settings for the node.
     *
     * @opensearch.internal
     */
    public static class DiscoverySettings {
        public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING = Setting.positiveTimeSetting(
            "discovery.initial_state_timeout",
            TimeValue.timeValueSeconds(30),
            Property.NodeScope
        );
    }

    /**
     * This logger instance is an instance field as opposed to a static field. This ensures that the field is not
     * initialized until an instance of Node is constructed, which is sure to happen after the logging infrastructure
     * has been initialized to include the hostname. If this field were static, then it would be initialized when the
     * class initializer runs. Alas, this happens too early, before logging is initialized as this class is referred to
     * in InternalSettingsPreparer#finalizeSettings, which runs when creating the Environment, before logging is
     * initialized.
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> breakerSettings,
        ClusterSettings clusterSettings
    ) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Custom ForkJoinWorkerThread that preserves the context ClassLoader of the creating thread
     * to ensure proper resource loading in worker threads.
     */
    public static class CustomForkJoinWorkerThread extends ForkJoinWorkerThread {
        public CustomForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            setContextClassLoader(Thread.currentThread().getContextClassLoader());
        }
    }

    private static long calculateFileCacheSize(String capacityRaw, long totalSpace) {
        try {
            RatioValue ratioValue = RatioValue.parseRatioValue(capacityRaw);
            return Math.round(totalSpace * ratioValue.getAsRatio());
        } catch (OpenSearchParseException e) {
            try {
                return ByteSizeValue.parseBytesSizeValue(capacityRaw, NODE_SEARCH_CACHE_SIZE_SETTING.getKey()).getBytes();
            } catch (OpenSearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
    }

    private static String validateFileCacheSize(String capacityRaw) {
        calculateFileCacheSize(capacityRaw, 0L);
        return capacityRaw;
    }

}
