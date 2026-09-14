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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.stats;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodeRole;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRoutingStats;
import org.codelibs.fesen.opensearch.cluster.service.ClusterManagerThrottlingStats;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.cache.service.NodeCacheStats;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.discovery.DiscoveryStats;
import org.codelibs.fesen.opensearch.http.HttpStats;
import org.codelibs.fesen.opensearch.index.SegmentReplicationRejectionStats;
import org.codelibs.fesen.opensearch.index.stats.IndexingPressureStats;
import org.codelibs.fesen.opensearch.index.stats.ShardIndexingPressureStats;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.codelibs.fesen.opensearch.indices.NodeIndicesStats;
import org.codelibs.fesen.opensearch.ingest.IngestStats;
import org.codelibs.fesen.opensearch.monitor.fs.FsInfo;
import org.codelibs.fesen.opensearch.monitor.jvm.JvmStats;
import org.codelibs.fesen.opensearch.monitor.os.OsStats;
import org.codelibs.fesen.opensearch.monitor.process.ProcessStats;
import org.codelibs.fesen.opensearch.node.AdaptiveSelectionStats;
import org.codelibs.fesen.opensearch.node.NodesResourceUsageStats;
import org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeStats;
import org.codelibs.fesen.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.codelibs.fesen.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.codelibs.fesen.opensearch.plugins.BlockCacheStats;
import org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.codelibs.fesen.opensearch.repositories.RepositoriesStats;
import org.codelibs.fesen.opensearch.script.ScriptCacheStats;
import org.codelibs.fesen.opensearch.script.ScriptStats;
import org.codelibs.fesen.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineStats;
import org.codelibs.fesen.opensearch.tasks.TaskCancellationStats;
import org.codelibs.fesen.opensearch.threadpool.ThreadPoolStats;
import org.codelibs.fesen.opensearch.transport.TransportStats;

import java.io.IOException;
import java.util.Map;

/**
 * Node statistics (dynamic, changes depending on when created).
 *
 * @opensearch.internal
 */
public class NodeStats extends BaseNodeResponse implements ToXContentFragment {

    private long timestamp;

    @Nullable
    private NodeIndicesStats indices;

    @Nullable
    private OsStats os;

    @Nullable
    private ProcessStats process;

    @Nullable
    private JvmStats jvm;

    @Nullable
    private ThreadPoolStats threadPool;

    @Nullable
    private FsInfo fs;

    @Nullable
    private TransportStats transport;

    @Nullable
    private HttpStats http;

    @Nullable
    private AllCircuitBreakerStats breaker;

    @Nullable
    private ScriptStats scriptStats;

    @Nullable
    private ScriptCacheStats scriptCacheStats;

    @Nullable
    private DiscoveryStats discoveryStats;

    @Nullable
    private IngestStats ingestStats;

    @Nullable
    private AdaptiveSelectionStats adaptiveSelectionStats;

    @Nullable
    private IndexingPressureStats indexingPressureStats;

    @Nullable
    private ShardIndexingPressureStats shardIndexingPressureStats;

    @Nullable
    private SearchBackpressureStats searchBackpressureStats;

    @Nullable
    private SegmentReplicationRejectionStats segmentReplicationRejectionStats;

    @Nullable
    private ClusterManagerThrottlingStats clusterManagerThrottlingStats;

    @Nullable
    private WeightedRoutingStats weightedRoutingStats;

    @Nullable
    private AggregateFileCacheStats fileCacheStats;

    /** Populated only when ?detailed is requested: FileCache-only stats (no block cache contribution). */
    @Nullable
    private AggregateFileCacheStats fileCacheOnlyStats;

    /** Populated only when ?detailed is requested: combined rollup across all BlockCache implementations. */
    @Nullable
    private BlockCacheStats blockCacheOnlyStats;

    @Nullable
    private TaskCancellationStats taskCancellationStats;

    @Nullable
    private SearchPipelineStats searchPipelineStats;

    @Nullable
    private NodesResourceUsageStats resourceUsageStats;

    @Nullable
    private RepositoriesStats repositoriesStats;

    @Nullable
    private AdmissionControlStats admissionControlStats;

    @Nullable
    private NodeCacheStats nodeCacheStats;

    @Nullable
    private RemoteStoreNodeStats remoteStoreNodeStats;

    @Nullable
    private NativeAllocatorPoolStats nativeAllocatorStats;

    /**
     * Process-level native-memory estimate captured on the data node hosting this {@code NodeStats}.
     * Computed once in {@link org.codelibs.fesen.opensearch.node.NodeService#stats} via
     * {@code OsProbe.getProcessNativeMemoryBytes()} and serialized over the wire so the coordinator
     * renders the source node's value, not its own. {@code -1} when the probe could not read
     * {@code /proc/self/status} (non-Linux platforms or restricted environments).
     */
    private long totalEstimatedNativeBytes;

    /**
     * Creates a new NodeStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public NodeStats(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        if (in.readBoolean()) {
            indices = new NodeIndicesStats(in);
        }
        os = in.readOptionalWriteable(OsStats::new);
        process = in.readOptionalWriteable(ProcessStats::new);
        jvm = in.readOptionalWriteable(JvmStats::new);
        threadPool = in.readOptionalWriteable(ThreadPoolStats::new);
        fs = in.readOptionalWriteable(FsInfo::new);
        transport = in.readOptionalWriteable(TransportStats::new);
        http = in.readOptionalWriteable(HttpStats::new);
        breaker = in.readOptionalWriteable(AllCircuitBreakerStats::new);
        scriptStats = in.readOptionalWriteable(ScriptStats::new);
        discoveryStats = in.readOptionalWriteable(DiscoveryStats::new);
        ingestStats = in.readOptionalWriteable(IngestStats::new);
        adaptiveSelectionStats = in.readOptionalWriteable(AdaptiveSelectionStats::new);
        scriptCacheStats = null;
        if (scriptStats != null) {
            scriptCacheStats = scriptStats.toScriptCacheStats();
        }
        indexingPressureStats = in.readOptionalWriteable(IndexingPressureStats::new);
        shardIndexingPressureStats = in.readOptionalWriteable(ShardIndexingPressureStats::new);

        if (in.getVersion().onOrAfter(Version.V_2_4_0)) {
            searchBackpressureStats = in.readOptionalWriteable(SearchBackpressureStats::new);
        } else {
            searchBackpressureStats = null;
        }

        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            clusterManagerThrottlingStats = in.readOptionalWriteable(ClusterManagerThrottlingStats::new);
        } else {
            clusterManagerThrottlingStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            weightedRoutingStats = in.readOptionalWriteable(WeightedRoutingStats::new);
        } else {
            weightedRoutingStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            fileCacheStats = in.readOptionalWriteable(AggregateFileCacheStats::new);
        } else {
            fileCacheStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            fileCacheOnlyStats = in.readOptionalWriteable(AggregateFileCacheStats::new);
            blockCacheOnlyStats = in.readOptionalWriteable(BlockCacheStats::new);
        } else {
            fileCacheOnlyStats = null;
            blockCacheOnlyStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            taskCancellationStats = in.readOptionalWriteable(TaskCancellationStats::new);
        } else {
            taskCancellationStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            searchPipelineStats = in.readOptionalWriteable(SearchPipelineStats::new);
        } else {
            searchPipelineStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            resourceUsageStats = in.readOptionalWriteable(NodesResourceUsageStats::new);
        } else {
            resourceUsageStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            segmentReplicationRejectionStats = in.readOptionalWriteable(SegmentReplicationRejectionStats::new);
        } else {
            segmentReplicationRejectionStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            repositoriesStats = in.readOptionalWriteable(RepositoriesStats::new);
        } else {
            repositoriesStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            admissionControlStats = in.readOptionalWriteable(AdmissionControlStats::new);
        } else {
            admissionControlStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
            nodeCacheStats = in.readOptionalWriteable(NodeCacheStats::new);
        } else {
            nodeCacheStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            remoteStoreNodeStats = in.readOptionalWriteable(RemoteStoreNodeStats::new);
        } else {
            remoteStoreNodeStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_8_0)) {
            nativeAllocatorStats = in.readOptionalWriteable(NativeAllocatorPoolStats::new);
        } else if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 wrote old-format NativeAllocatorPoolStats (3 VLongs + pools with 4 fields); read and discard.
            in.readOptionalWriteable(NativeAllocatorPoolStats::readAndDiscardV3_7);
            nativeAllocatorStats = null;
        } else {
            nativeAllocatorStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 wrote AnalyticsBackendNativeMemoryStats here; read and discard.
            in.readOptionalWriteable(AnalyticsBackendNativeMemoryStats::new);
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            totalEstimatedNativeBytes = in.readLong();
        } else {
            totalEstimatedNativeBytes = -1L;
        }
    }

    /**
     * Creates a new NodeStats.
     *
     * @param node the node
     * @param timestamp the timestamp
     * @param indices the indices
     * @param os the OS
     * @param process the process
     * @param jvm the JVM
     * @param threadPool the thread pool
     * @param fs the fs
     * @param transport the transport
     * @param http the HTTP
     * @param breaker the breaker
     * @param scriptStats the script stats
     * @param discoveryStats the discovery stats
     * @param ingestStats the ingest stats
     * @param adaptiveSelectionStats the adaptive selection stats
     * @param resourceUsageStats the resource usage stats
     * @param scriptCacheStats the script cache stats
     * @param indexingPressureStats the indexing pressure stats
     * @param shardIndexingPressureStats the shard indexing pressure stats
     * @param searchBackpressureStats the search backpressure stats
     * @param clusterManagerThrottlingStats the cluster manager throttling stats
     * @param weightedRoutingStats the weighted routing stats
     * @param fileCacheStats the file cache stats
     * @param fileCacheOnlyStats the file cache only stats
     * @param blockCacheOnlyStats the block cache only stats
     * @param taskCancellationStats the task cancellation stats
     * @param searchPipelineStats the search pipeline stats
     * @param segmentReplicationRejectionStats the segment replication rejection stats
     * @param repositoriesStats the repositories stats
     * @param admissionControlStats the admission control stats
     * @param nodeCacheStats the node cache stats
     * @param remoteStoreNodeStats the remote store node stats
     * @param nativeAllocatorStats the native allocator stats
     * @param totalEstimatedNativeBytes the total estimated native bytes
     */
    public NodeStats(
        DiscoveryNode node,
        long timestamp,
        @Nullable NodeIndicesStats indices,
        @Nullable OsStats os,
        @Nullable ProcessStats process,
        @Nullable JvmStats jvm,
        @Nullable ThreadPoolStats threadPool,
        @Nullable FsInfo fs,
        @Nullable TransportStats transport,
        @Nullable HttpStats http,
        @Nullable AllCircuitBreakerStats breaker,
        @Nullable ScriptStats scriptStats,
        @Nullable DiscoveryStats discoveryStats,
        @Nullable IngestStats ingestStats,
        @Nullable AdaptiveSelectionStats adaptiveSelectionStats,
        @Nullable NodesResourceUsageStats resourceUsageStats,
        @Nullable ScriptCacheStats scriptCacheStats,
        @Nullable IndexingPressureStats indexingPressureStats,
        @Nullable ShardIndexingPressureStats shardIndexingPressureStats,
        @Nullable SearchBackpressureStats searchBackpressureStats,
        @Nullable ClusterManagerThrottlingStats clusterManagerThrottlingStats,
        @Nullable WeightedRoutingStats weightedRoutingStats,
        @Nullable AggregateFileCacheStats fileCacheStats,
        @Nullable AggregateFileCacheStats fileCacheOnlyStats,
        @Nullable BlockCacheStats blockCacheOnlyStats,
        @Nullable TaskCancellationStats taskCancellationStats,
        @Nullable SearchPipelineStats searchPipelineStats,
        @Nullable SegmentReplicationRejectionStats segmentReplicationRejectionStats,
        @Nullable RepositoriesStats repositoriesStats,
        @Nullable AdmissionControlStats admissionControlStats,
        @Nullable NodeCacheStats nodeCacheStats,
        @Nullable RemoteStoreNodeStats remoteStoreNodeStats,
        @Nullable NativeAllocatorPoolStats nativeAllocatorStats,
        long totalEstimatedNativeBytes
    ) {
        super(node);
        this.timestamp = timestamp;
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.fs = fs;
        this.transport = transport;
        this.http = http;
        this.breaker = breaker;
        this.scriptStats = scriptStats;
        this.discoveryStats = discoveryStats;
        this.ingestStats = ingestStats;
        this.adaptiveSelectionStats = adaptiveSelectionStats;
        this.resourceUsageStats = resourceUsageStats;
        this.scriptCacheStats = scriptCacheStats;
        this.indexingPressureStats = indexingPressureStats;
        this.shardIndexingPressureStats = shardIndexingPressureStats;
        this.searchBackpressureStats = searchBackpressureStats;
        this.clusterManagerThrottlingStats = clusterManagerThrottlingStats;
        this.weightedRoutingStats = weightedRoutingStats;
        this.fileCacheStats = fileCacheStats;
        this.fileCacheOnlyStats = fileCacheOnlyStats;
        this.blockCacheOnlyStats = blockCacheOnlyStats;
        this.taskCancellationStats = taskCancellationStats;
        this.searchPipelineStats = searchPipelineStats;
        this.segmentReplicationRejectionStats = segmentReplicationRejectionStats;
        this.repositoriesStats = repositoriesStats;
        this.admissionControlStats = admissionControlStats;
        this.nodeCacheStats = nodeCacheStats;
        this.remoteStoreNodeStats = remoteStoreNodeStats;
        this.nativeAllocatorStats = nativeAllocatorStats;
        this.totalEstimatedNativeBytes = totalEstimatedNativeBytes;
    }

    /**
     * Returns the timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Indices level stats.
     *
     * @return the indices
     */
    @Nullable
    public NodeIndicesStats getIndices() {
        return this.indices;
    }

    /**
     * Operating System level statistics.
     *
     * @return the OS
     */
    @Nullable
    public OsStats getOs() {
        return this.os;
    }

    /**
     * Process level statistics.
     *
     * @return the process
     */
    @Nullable
    public ProcessStats getProcess() {
        return process;
    }

    /**
     * JVM level statistics.
     *
     * @return the JVM
     */
    @Nullable
    public JvmStats getJvm() {
        return jvm;
    }

    /**
     * Thread Pool level statistics.
     *
     * @return the thread pool
     */
    @Nullable
    public ThreadPoolStats getThreadPool() {
        return this.threadPool;
    }

    /**
     * File system level stats.
     *
     * @return the fs
     */
    @Nullable
    public FsInfo getFs() {
        return fs;
    }

    /**
     * Returns the transport.
     *
     * @return the transport
     */
    @Nullable
    public TransportStats getTransport() {
        return this.transport;
    }

    /**
     * Returns the HTTP.
     *
     * @return the HTTP
     */
    @Nullable
    public HttpStats getHttp() {
        return this.http;
    }

    /**
     * Returns the breaker.
     *
     * @return the breaker
     */
    @Nullable
    public AllCircuitBreakerStats getBreaker() {
        return this.breaker;
    }

    /**
     * Returns the script stats.
     *
     * @return the script stats
     */
    @Nullable
    public ScriptStats getScriptStats() {
        return this.scriptStats;
    }

    /**
     * Returns the discovery stats.
     *
     * @return the discovery stats
     */
    @Nullable
    public DiscoveryStats getDiscoveryStats() {
        return this.discoveryStats;
    }

    /**
     * Returns the ingest stats.
     *
     * @return the ingest stats
     */
    @Nullable
    public IngestStats getIngestStats() {
        return ingestStats;
    }

    /**
     * Returns the adaptive selection stats.
     *
     * @return the adaptive selection stats
     */
    @Nullable
    public AdaptiveSelectionStats getAdaptiveSelectionStats() {
        return adaptiveSelectionStats;
    }

    /**
     * Returns the resource usage stats.
     *
     * @return the resource usage stats
     */
    @Nullable
    public NodesResourceUsageStats getResourceUsageStats() {
        return resourceUsageStats;
    }

    /**
     * Returns the script cache stats.
     *
     * @return the script cache stats
     */
    @Nullable
    public ScriptCacheStats getScriptCacheStats() {
        return scriptCacheStats;
    }

    /**
     * Returns the indexing pressure stats.
     *
     * @return the indexing pressure stats
     */
    @Nullable
    public IndexingPressureStats getIndexingPressureStats() {
        return indexingPressureStats;
    }

    /**
     * Returns the shard indexing pressure stats.
     *
     * @return the shard indexing pressure stats
     */
    @Nullable
    public ShardIndexingPressureStats getShardIndexingPressureStats() {
        return shardIndexingPressureStats;
    }

    /**
     * Returns the search backpressure stats.
     *
     * @return the search backpressure stats
     */
    @Nullable
    public SearchBackpressureStats getSearchBackpressureStats() {
        return searchBackpressureStats;
    }

    /**
     * Returns the cluster manager throttling stats.
     *
     * @return the cluster manager throttling stats
     */
    @Nullable
    public ClusterManagerThrottlingStats getClusterManagerThrottlingStats() {
        return clusterManagerThrottlingStats;
    }

    /**
     * Returns the weighted routing stats.
     *
     * @return the weighted routing stats
     */
    public WeightedRoutingStats getWeightedRoutingStats() {
        return weightedRoutingStats;
    }

    /**
     * Returns the file cache stats.
     *
     * @return the file cache stats
     */
    public AggregateFileCacheStats getFileCacheStats() {
        return fileCacheStats;
    }

    /**
     * Returns the file cache only stats.
     *
     * @return the file cache only stats
     */
    @Nullable
    public AggregateFileCacheStats getFileCacheOnlyStats() {
        return fileCacheOnlyStats;
    }

    /**
     * Returns the block cache only stats.
     *
     * @return the block cache only stats
     */
    @Nullable
    public BlockCacheStats getBlockCacheOnlyStats() {
        return blockCacheOnlyStats;
    }

    /**
     * Returns the task cancellation stats.
     *
     * @return the task cancellation stats
     */
    @Nullable
    public TaskCancellationStats getTaskCancellationStats() {
        return taskCancellationStats;
    }

    /**
     * Returns the search pipeline stats.
     *
     * @return the search pipeline stats
     */
    @Nullable
    public SearchPipelineStats getSearchPipelineStats() {
        return searchPipelineStats;
    }

    /**
     * Returns the segment replication rejection stats.
     *
     * @return the segment replication rejection stats
     */
    @Nullable
    public SegmentReplicationRejectionStats getSegmentReplicationRejectionStats() {
        return segmentReplicationRejectionStats;
    }

    /**
     * Returns the repositories stats.
     *
     * @return the repositories stats
     */
    @Nullable
    public RepositoriesStats getRepositoriesStats() {
        return repositoriesStats;
    }

    /**
     * Returns the admission control stats.
     *
     * @return the admission control stats
     */
    @Nullable
    public AdmissionControlStats getAdmissionControlStats() {
        return admissionControlStats;
    }

    /**
     * Returns the node cache stats.
     *
     * @return the node cache stats
     */
    @Nullable
    public NodeCacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    /**
     * Returns the remote store node stats.
     *
     * @return the remote store node stats
     */
    @Nullable
    public RemoteStoreNodeStats getRemoteStoreNodeStats() {
        return remoteStoreNodeStats;
    }

    /**
     * Returns the native allocator pool stats (Arrow allocator), or {@code null} if not available.
     *
     * @return the native allocator stats
     */
    @Nullable
    public NativeAllocatorPoolStats getNativeAllocatorStats() {
        return nativeAllocatorStats;
    }

    /**
     * Returns the process-level native-memory estimate captured on this node
     * (RssAnon - JVM heap committed - JVM non-heap committed), or {@code -1} when the probe
     * could not read {@code /proc/self/status}.
     *
     * @return the total estimated native bytes
     */
    public long getTotalEstimatedNativeBytes() {
        return totalEstimatedNativeBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        if (indices == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indices.writeTo(out);
        }
        out.writeOptionalWriteable(os);
        out.writeOptionalWriteable(process);
        out.writeOptionalWriteable(jvm);
        out.writeOptionalWriteable(threadPool);
        out.writeOptionalWriteable(fs);
        out.writeOptionalWriteable(transport);
        out.writeOptionalWriteable(http);
        out.writeOptionalWriteable(breaker);
        out.writeOptionalWriteable(scriptStats);
        out.writeOptionalWriteable(discoveryStats);
        out.writeOptionalWriteable(ingestStats);
        out.writeOptionalWriteable(adaptiveSelectionStats);
        out.writeOptionalWriteable(indexingPressureStats);
        out.writeOptionalWriteable(shardIndexingPressureStats);

        if (out.getVersion().onOrAfter(Version.V_2_4_0)) {
            out.writeOptionalWriteable(searchBackpressureStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeOptionalWriteable(clusterManagerThrottlingStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeOptionalWriteable(weightedRoutingStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeOptionalWriteable(fileCacheStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeOptionalWriteable(fileCacheOnlyStats);
            out.writeOptionalWriteable(blockCacheOnlyStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeOptionalWriteable(taskCancellationStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeOptionalWriteable(searchPipelineStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(resourceUsageStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(segmentReplicationRejectionStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(repositoriesStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(admissionControlStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
            out.writeOptionalWriteable(nodeCacheStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalWriteable(remoteStoreNodeStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeOptionalWriteable(nativeAllocatorStats);
        } else if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: write old-format NativeAllocatorPoolStats for V_3_7_0 nodes
            NativeAllocatorPoolStats.writeV3_7(out, nativeAllocatorStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 expects AnalyticsBackendNativeMemoryStats here; write null.
            out.writeOptionalWriteable(null);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeLong(totalEstimatedNativeBytes);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getAddress());

        builder.startArray("roles");
        for (DiscoveryNodeRole role : getNode().getRoles()) {
            builder.value(role.roleName());
        }
        builder.endArray();

        if (!getNode().getAttributes().isEmpty()) {
            builder.startObject("attributes");
            for (Map.Entry<String, String> attrEntry : getNode().getAttributes().entrySet()) {
                builder.field(attrEntry.getKey(), attrEntry.getValue());
            }
            builder.endObject();
        }

        if (getIndices() != null) {
            getIndices().toXContent(builder, params);
        }
        if (getOs() != null) {
            getOs().toXContent(builder, params);
        }
        if (getProcess() != null) {
            getProcess().toXContent(builder, params);
        }
        if (getJvm() != null) {
            getJvm().toXContent(builder, params);
        }
        if (getThreadPool() != null) {
            getThreadPool().toXContent(builder, params);
        }
        if (getFs() != null) {
            getFs().toXContent(builder, params);
        }
        if (getTransport() != null) {
            getTransport().toXContent(builder, params);
        }
        if (getHttp() != null) {
            getHttp().toXContent(builder, params);
        }
        if (getBreaker() != null) {
            getBreaker().toXContent(builder, params);
        }
        if (getScriptStats() != null) {
            getScriptStats().toXContent(builder, params);
        }
        if (getDiscoveryStats() != null) {
            getDiscoveryStats().toXContent(builder, params);
        }
        if (getIngestStats() != null) {
            getIngestStats().toXContent(builder, params);
        }
        if (getAdaptiveSelectionStats() != null) {
            getAdaptiveSelectionStats().toXContent(builder, params);
        }
        if (getScriptCacheStats() != null) {
            getScriptCacheStats().toXContent(builder, params);
        }
        if (getIndexingPressureStats() != null) {
            getIndexingPressureStats().toXContent(builder, params);
        }
        if (getShardIndexingPressureStats() != null) {
            getShardIndexingPressureStats().toXContent(builder, params);
        }
        if (getSearchBackpressureStats() != null) {
            getSearchBackpressureStats().toXContent(builder, params);
        }
        if (getClusterManagerThrottlingStats() != null) {
            getClusterManagerThrottlingStats().toXContent(builder, params);
        }
        if (getWeightedRoutingStats() != null) {
            getWeightedRoutingStats().toXContent(builder, params);
        }
        if (getFileCacheStats() != null) {
            getFileCacheStats().toXContent(builder, params);
        }
        if (getFileCacheOnlyStats() != null) {
            builder.startObject("file_cache");
            getFileCacheOnlyStats().getOverallFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getFullFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getBlockFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getPinnedFileCacheStats().toXContent(builder, params);
            builder.endObject();
        }
        if (getBlockCacheOnlyStats() != null) {
            getBlockCacheOnlyStats().toXContent(builder, params);
        }
        if (getTaskCancellationStats() != null) {
            getTaskCancellationStats().toXContent(builder, params);
        }
        if (getSearchPipelineStats() != null) {
            getSearchPipelineStats().toXContent(builder, params);
        }
        if (getResourceUsageStats() != null) {
            getResourceUsageStats().toXContent(builder, params);
        }
        if (getSegmentReplicationRejectionStats() != null) {
            getSegmentReplicationRejectionStats().toXContent(builder, params);
        }

        if (getRepositoriesStats() != null) {
            getRepositoriesStats().toXContent(builder, params);
        }
        if (getAdmissionControlStats() != null) {
            getAdmissionControlStats().toXContent(builder, params);
        }
        if (getNodeCacheStats() != null) {
            getNodeCacheStats().toXContent(builder, params);
        }
        if (getRemoteStoreNodeStats() != null) {
            getRemoteStoreNodeStats().toXContent(builder, params);
        }
        // total_estimated_bytes ≈ RssAnon - JVM heap committed - JVM non-heap committed.
        // native_memory: unified view of all native memory pools and jemalloc stats.
        // NativeAllocatorPoolStats now includes jemalloc allocated/resident + all pools.
        builder.startObject("native_memory");
        builder.field("total_estimated_bytes", totalEstimatedNativeBytes);
        if (getNativeAllocatorStats() != null) {
            NativeAllocatorPoolStats stats = getNativeAllocatorStats();
            builder.startObject("runtime");
            builder.field("allocated_bytes", stats.getNativeAllocatedBytes());
            builder.field("resident_bytes", stats.getNativeResidentBytes());
            builder.endObject();
            builder.startObject("memory_pools");
            for (var entry : stats.getGroupedStats().entrySet()) {
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
