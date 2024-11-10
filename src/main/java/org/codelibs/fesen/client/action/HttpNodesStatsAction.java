/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.coordination.PendingClusterStateStats;
import org.opensearch.cluster.coordination.PublishClusterStateStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.cluster.service.ClusterManagerThrottlingStats;
import org.opensearch.cluster.service.ClusterStateStats;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.http.HttpStats;
import org.opensearch.index.SegmentReplicationRejectionStats;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.recovery.RecoveryStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.shard.IndexingStats.Stats.DocStatusStats;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.index.stats.ShardIndexingPressureStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.ingest.IngestStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.jvm.JvmStats.MemoryPoolGcStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.node.AdaptiveSelectionStats;
import org.opensearch.node.NodesResourceUsageStats;
import org.opensearch.node.remotestore.RemoteStoreNodeStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.opensearch.repositories.RepositoriesStats;
import org.opensearch.script.ScriptCacheStats;
import org.opensearch.script.ScriptStats;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.stats.SearchShardTaskStats;
import org.opensearch.search.backpressure.stats.SearchTaskStats;
import org.opensearch.search.pipeline.SearchPipelineStats;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.tasks.SearchShardTaskCancellationStats;
import org.opensearch.tasks.TaskCancellationStats;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;

public class HttpNodesStatsAction extends HttpAction {

    protected NodesStatsAction action;

    protected ClusterSettings clusterSettings;

    public HttpNodesStatsAction(final HttpClient client, final NodesStatsAction action) {
        super(client);
        this.action = action;
        final Map<String, Setting<?>> nodeSettings = new HashMap<>();
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            if (setting.hasNodeScope()) {
                nodeSettings.put(setting.getKey(), setting);
            }
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            if (setting.hasNodeScope()) {
                nodeSettings.put(setting.getKey(), setting);
            }
        }
        for (Setting<?> setting : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            if (setting.hasNodeScope()) {
                nodeSettings.put(setting.getKey(), setting);
            }
        }
        this.clusterSettings = new ClusterSettings(client.settings(), new HashSet<>(nodeSettings.values()), Collections.emptySet());
    }

    public void execute(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final NodesStatsResponse nodesStatsResponse = fromXContent(parser);
                listener.onResponse(nodesStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected NodesStatsResponse fromXContent(final XContentParser parser) throws IOException {
        List<NodeStats> nodes = Collections.emptyList();
        String fieldName = null;
        ClusterName clusterName = ClusterName.DEFAULT;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_nodes".equals(fieldName)) {
                    parseNodeResults(parser);
                } else if ("nodes".equals(fieldName)) {
                    parser.nextToken();
                    nodes = parseNodes(parser);
                }
            } else if ((token == XContentParser.Token.VALUE_STRING) && "cluster_name".equals(fieldName)) {
                clusterName = new ClusterName(parser.text());
            }
            parser.nextToken();
        }
        return new NodesStatsResponse(clusterName, nodes, Collections.emptyList());
    }

    protected List<NodeStats> parseNodes(final XContentParser parser) throws IOException {
        final List<NodeStats> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                list.add(parseNodeStats(parser, fieldName));
            }
            parser.nextToken();
        }
        return list;
    }

    protected NodeStats parseNodeStats(final XContentParser parser, final String nodeId) throws IOException {
        new ArrayList<>();
        String fieldName = null;
        String nodeName = "";
        long timestamp = 0;
        final Set<DiscoveryNodeRole> roles = new HashSet<>();
        NodeIndicesStats indices = null;
        OsStats os = null;
        ProcessStats process = null;
        JvmStats jvm = null;
        ThreadPoolStats threadPool = null;
        FsInfo fs = null;
        TransportStats transport = null;
        HttpStats http = null;
        AllCircuitBreakerStats breaker = null;
        ScriptStats scriptStats = null;
        DiscoveryStats discoveryStats = null;
        IngestStats ingestStats = null;
        AdaptiveSelectionStats adaptiveSelectionStats = null;
        NodesResourceUsageStats resourceUsageStats = null;
        ScriptCacheStats scriptCacheStats = null;
        IndexingPressureStats indexingPressureStats = null;
        ShardIndexingPressureStats shardIndexingPressureStats = null;
        SearchBackpressureStats searchBackpressureStats = null;
        ClusterManagerThrottlingStats clusterManagerThrottlingStats = null;
        WeightedRoutingStats weightedRoutingStats = null;
        FileCacheStats fileCacheStats = null;
        TaskCancellationStats taskCancellationStats = null;
        SearchPipelineStats searchPipelineStats = null;
        SegmentReplicationRejectionStats segmentReplicationRejectionStats = null; // TODO
        RepositoriesStats repositoriesStats = null; // TODO
        AdmissionControlStats admissionControlStats = null; // TODO
        NodeCacheStats nodeCacheStats = null; // TODO
        RemoteStoreNodeStats remoteStoreNodeStats = null; // TODO
        final Map<String, String> attributes = new HashMap<>();
        XContentParser.Token token;
        TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("indices".equals(fieldName)) {
                    indices = parseNodeIndicesStats(parser);
                } else if ("os".equals(fieldName)) {
                    os = parseOsStats(parser);
                } else if ("process".equals(fieldName)) {
                    process = parseProcessStats(parser);
                } else if ("jvm".equals(fieldName)) {
                    jvm = parseJvmStats(parser);
                } else if ("thread_pool".equals(fieldName)) {
                    threadPool = parseThreadPoolStats(parser);
                } else if ("fs".equals(fieldName)) {
                    fs = parseFsInfo(parser);
                } else if ("transport".equals(fieldName)) {
                    transport = parseTransportStats(parser);
                } else if ("http".equals(fieldName)) {
                    http = parseHttpStats(parser);
                } else if ("breakers".equals(fieldName)) {
                    breaker = parseAllCircuitBreakerStats(parser);
                } else if ("script".equals(fieldName)) {
                    scriptStats = parseScriptStats(parser);
                } else if ("discovery".equals(fieldName)) {
                    discoveryStats = parseDiscoveryStats(parser);
                } else if ("ingest".equals(fieldName)) {
                    ingestStats = parseIngestStats(parser);
                } else if ("adaptive_selection".equals(fieldName)) {
                    adaptiveSelectionStats = parseAdaptiveSelectionStats(parser);
                } else if ("script_cache".equals(fieldName)) {
                    scriptCacheStats = parseScriptCacheStats(parser);
                } else if ("indexing_pressure".equals(fieldName)) {
                    indexingPressureStats = parseIndexingPressureStats(parser);
                } else if ("shard_indexing_pressure".equals(fieldName)) {
                    shardIndexingPressureStats = parseShardIndexingPressureStats(parser);
                } else if ("search_backpressure".equals(fieldName)) {
                    searchBackpressureStats = parseSearchBackpressureStats(parser);
                } else if ("cluster_manager_throttling".equals(fieldName)) {
                    clusterManagerThrottlingStats = parseClusterManagerThrottlingStats(parser);
                } else if ("weighted_routing".equals(fieldName)) {
                    weightedRoutingStats = parseWeightedRoutingStats(parser);
                } else if ("file_cache".equals(fieldName)) {
                    fileCacheStats = parseFileCacheStats(parser);
                } else if ("task_cancellation".equals(fieldName)) {
                    taskCancellationStats = parseTaskCancellationStats(parser);
                } else if ("search_pipeline".equals(fieldName)) {
                    searchPipelineStats = parseSearchPipelineStats(parser);
                } else {
                    consumeObject(parser);
                }

            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("name".equals(fieldName)) {
                    nodeName = parser.text();
                } else if ("transport_address".equals(fieldName)) {
                    transportAddress = parseTransportAddress(parser.text());
                }
            }
            // TODO roles
            parser.nextToken();
        }
        final DiscoveryNode node = new DiscoveryNode(nodeName, nodeId, transportAddress, attributes, roles, Version.CURRENT);
        return new NodeStats(node, //
                timestamp, //
                indices, //
                os, //
                process, //
                jvm, //
                threadPool, //
                fs, //
                transport, //
                http, //
                breaker, //
                scriptStats, //
                discoveryStats, //
                ingestStats, //
                adaptiveSelectionStats, //
                resourceUsageStats, //
                scriptCacheStats, //
                indexingPressureStats, //
                shardIndexingPressureStats, //
                searchBackpressureStats, //
                clusterManagerThrottlingStats, //
                weightedRoutingStats, //
                fileCacheStats, //
                taskCancellationStats, //
                searchPipelineStats, //
                segmentReplicationRejectionStats, //
                repositoriesStats, //
                admissionControlStats, //
                nodeCacheStats, //
                remoteStoreNodeStats);
    }

    public static TransportAddress parseTransportAddress(final String addr) {
        try {
            if (addr.startsWith("[")) {
                final String[] values = addr.split("\\]:");
                int port = 0;
                if (values.length > 1) {
                    port = Integer.parseInt(values[1]);
                }
                return new TransportAddress(InetAddress.getByName(values[0].replace('[', ' ').replace(']', ' ').trim()), port);
            }
            final String[] values = addr.split(":");
            int port = 0;
            if (values.length > 1) {
                port = Integer.parseInt(values[1]);
            }
            return new TransportAddress(InetAddress.getByName(values[0]), port);
        } catch (final Exception e) {
            return new TransportAddress(TransportAddress.META_ADDRESS, 0);
        }
    }

    protected AdaptiveSelectionStats parseAdaptiveSelectionStats(final XContentParser parser) throws IOException {
        consumeObject(parser); // TODO
        return new AdaptiveSelectionStats(Collections.emptyMap(), Collections.emptyMap());
    }

    protected ScriptCacheStats parseScriptCacheStats(final XContentParser parser) throws IOException {
        consumeObject(parser); // TODO
        return new ScriptCacheStats(Collections.emptyMap());
    }

    protected IndexingPressureStats parseIndexingPressureStats(final XContentParser parser) throws IOException {
        consumeObject(parser); // TODO
        return new IndexingPressureStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    protected ShardIndexingPressureStats parseShardIndexingPressureStats(final XContentParser parser) throws IOException {
        consumeObject(parser); // TODO
        return new ShardIndexingPressureStats(Collections.emptyMap(), 0, 0, 0, false, false);
    }

    protected SearchBackpressureStats parseSearchBackpressureStats(final XContentParser parser) throws IOException {
        SearchBackpressureMode mode = SearchBackpressureMode.DISABLED;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("search_task".equals(fieldName) || "search_shard_task".equals(fieldName)) {
                    consumeObject(parser); // TODO
                } else if ("mode".equals(fieldName)) {
                    mode = SearchBackpressureMode.valueOf(parser.text());
                }
            }
            parser.nextToken();
        }
        return new SearchBackpressureStats(new SearchTaskStats(0, 0, Collections.emptyMap()),
                new SearchShardTaskStats(0, 0, Collections.emptyMap()), mode);
    }

    protected ClusterManagerThrottlingStats parseClusterManagerThrottlingStats(final XContentParser parser) throws IOException {
        consumeObject(parser); // TODO
        return new ClusterManagerThrottlingStats();
    }

    protected WeightedRoutingStats parseWeightedRoutingStats(final XContentParser parser) throws IOException {
        int failOpenCount = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "stats".equals(fieldName)) {
                failOpenCount = parseFailOpenCount(parser);
            }
            parser.nextToken();
        }
        final WeightedRoutingStats stats = WeightedRoutingStats.getInstance();
        stats.resetFailOpenCount();
        for (int i = 0; i < failOpenCount; i++) {
            stats.updateFailOpenCount();
        }
        return stats;
    }

    protected int parseFailOpenCount(final XContentParser parser) throws IOException {
        int failOpenCount = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "fail_open_count".equals(fieldName)) {
                failOpenCount = parser.intValue();
            }
            parser.nextToken();
        }
        return failOpenCount;
    }

    protected FileCacheStats parseFileCacheStats(final XContentParser parser) throws IOException {
        long timestamp = 0;
        long active = 0;
        long total = 0;
        long used = 0;
        long evicted = 0;
        long hits = 0;
        long misses = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                } else if ("active_in_bytes".equals(fieldName)) {
                    active = parser.longValue();
                } else if ("total_in_bytes".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("used_in_bytes".equals(fieldName)) {
                    used = parser.longValue();
                } else if ("evictions_in_bytes".equals(fieldName)) {
                    evicted = parser.longValue();
                } else if ("hit_count".equals(fieldName)) {
                    hits = parser.longValue();
                } else if ("miss_count".equals(fieldName)) {
                    misses = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new FileCacheStats(timestamp, active, total, used, evicted, hits, misses);
    }

    protected TaskCancellationStats parseTaskCancellationStats(final XContentParser parser) throws IOException {
        SearchShardTaskCancellationStats searchShardTaskCancellationStats = null;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "search_shard_task".equals(fieldName)) {
                searchShardTaskCancellationStats = parseSearchShardTaskCancellationStats(parser);
            }
            parser.nextToken();
        }
        return new TaskCancellationStats(searchShardTaskCancellationStats);
    }

    protected SearchShardTaskCancellationStats parseSearchShardTaskCancellationStats(final XContentParser parser) throws IOException {
        long currentLongRunningCancelledTaskCount = 0;
        long totalLongRunningCancelledTaskCount = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("current_count_post_cancel".equals(fieldName)) {
                    currentLongRunningCancelledTaskCount = parser.longValue();
                } else if ("total_count_post_cancel".equals(fieldName)) {
                    totalLongRunningCancelledTaskCount = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new SearchShardTaskCancellationStats(currentLongRunningCancelledTaskCount, totalLongRunningCancelledTaskCount);
    }

    protected SearchPipelineStats parseSearchPipelineStats(final XContentParser parser) throws IOException {
        OperationStats totalRequestStats = null;
        OperationStats totalResponseStats = null;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total_request".equals(fieldName)) {
                    totalRequestStats = parseOperationStats(parser);
                } else if ("total_response".equals(fieldName)) {
                    totalResponseStats = parseOperationStats(parser);
                } else if ("pipelines".equals(fieldName)) {
                    consumeObject(parser); // TODO
                }
            }
            parser.nextToken();
        }
        return new SearchPipelineStats(totalRequestStats, totalResponseStats, Collections.emptyList(), Collections.emptyMap());
    }

    protected OperationStats parseOperationStats(final XContentParser parser) throws IOException {
        long count = 0;
        long totalTimeInMillis = 0;
        long current = 0;
        long failedCount = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("count".equals(fieldName)) {
                    count = parser.longValue();
                } else if ("time_in_millis".equals(fieldName)) {
                    totalTimeInMillis = parser.longValue();
                } else if ("current".equals(fieldName)) {
                    current = parser.longValue();
                } else if ("failed".equals(fieldName)) {
                    failedCount = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new OperationStats(count, totalTimeInMillis, current, failedCount);
    }

    protected IngestStats parseIngestStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        OperationStats totalStats = null;
        final List<IngestStats.PipelineStat> pipelineStats = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("total".equals(fieldName)) {
                    long ingestCount = 0;
                    long ingestTimeInMillis = 0;
                    long ingestCurrent = 0;
                    long ingestFailedCount = 0;
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("count".equals(fieldName)) {
                                ingestCount = parser.longValue();
                            } else if ("time_in_millis".equals(fieldName)) {
                                ingestTimeInMillis = parser.longValue();
                            } else if ("current".equals(fieldName)) {
                                ingestCurrent = parser.longValue();
                            } else if ("failed".equals(fieldName)) {
                                ingestFailedCount = parser.longValue();
                            }
                        }
                        parser.nextToken();
                    }
                    totalStats = new OperationStats(ingestCount, ingestTimeInMillis, ingestCurrent, ingestFailedCount);
                } else if ("pipelines".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            long ingestCount = 0;
                            long ingestTimeInMillis = 0;
                            long ingestCurrent = 0;
                            long ingestFailedCount = 0;
                            final String name = fieldName;
                            while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                    if ("count".equals(fieldName)) {
                                        ingestCount = parser.longValue();
                                    } else if ("time_in_millis".equals(fieldName)) {
                                        ingestTimeInMillis = parser.longValue();
                                    } else if ("current".equals(fieldName)) {
                                        ingestCurrent = parser.longValue();
                                    } else if ("failed".equals(fieldName)) {
                                        ingestFailedCount = parser.longValue();
                                    }
                                }
                                parser.nextToken();
                            }

                            pipelineStats.add(new IngestStats.PipelineStat(name,
                                    new OperationStats(ingestCount, ingestTimeInMillis, ingestCurrent, ingestFailedCount)));
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            }
            parser.nextToken();
        }
        return new IngestStats(totalStats, pipelineStats, Collections.emptyMap());
    }

    protected DiscoveryStats parseDiscoveryStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        PendingClusterStateStats queueStats = null;
        PublishClusterStateStats publishStats = null;
        ClusterStateStats clusterStateStats = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("cluster_state_queue".equals(fieldName)) {
                    queueStats = parsePendingClusterStateStats(parser);
                } else if ("published_cluster_states".equals(fieldName)) {
                    publishStats = parsePublishClusterStateStats(parser);
                } else if ("cluster_state_stats".equals(fieldName)) {
                    clusterStateStats = parseClusterStateStats(parser);
                } else {
                    consumeObject(parser);
                }
            }
            parser.nextToken();
        }
        return new DiscoveryStats(queueStats, publishStats, clusterStateStats);
    }

    protected ClusterStateStats parseClusterStateStats(XContentParser parser) throws IOException {
        String fieldName = null;
        long updateSuccess = 0;
        long updateTotalTimeInMillis = 0;
        long updateFailed = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("overall".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("update_count".equals(fieldName)) {
                                updateSuccess = parser.longValue();
                            } else if ("total_time_in_millis".equals(fieldName)) {
                                updateTotalTimeInMillis = parser.longValue();
                            } else if ("failed_count".equals(fieldName)) {
                                updateFailed = parser.longValue();
                            }
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            }
            parser.nextToken();
        }
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeLong(updateSuccess);
            out.writeLong(updateTotalTimeInMillis);
            out.writeLong(updateFailed);
            out.writeInt(0); // PersistedStateStats
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray()))) {
                return new ClusterStateStats(in);
            }
        }
    }

    protected PublishClusterStateStats parsePublishClusterStateStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long fullClusterStateReceivedCount = 0;
        long incompatibleClusterStateDiffReceivedCount = 0;
        long compatibleClusterStateDiffReceivedCount = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("full_states".equals(fieldName)) {
                    fullClusterStateReceivedCount = parser.intValue();
                } else if ("incompatible_diffs".equals(fieldName)) {
                    incompatibleClusterStateDiffReceivedCount = parser.intValue();
                } else if ("compatible_diffs".equals(fieldName)) {
                    compatibleClusterStateDiffReceivedCount = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return new PublishClusterStateStats(fullClusterStateReceivedCount, incompatibleClusterStateDiffReceivedCount,
                compatibleClusterStateDiffReceivedCount);
    }

    protected PendingClusterStateStats parsePendingClusterStateStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        int total = 0;
        int pending = 0;
        int committed = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    total = parser.intValue();
                } else if ("pending".equals(fieldName)) {
                    pending = parser.intValue();
                } else if ("committed".equals(fieldName)) {
                    committed = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return new PendingClusterStateStats(total, pending, committed);
    }

    protected ScriptStats parseScriptStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("compilations".equals(fieldName)) {
                    compilations = parser.longValue();
                } else if ("cache_evictions".equals(fieldName)) {
                    cacheEvictions = parser.longValue();
                } else if ("compilation_limit_triggered".equals(fieldName)) {
                    compilationLimitTriggered = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new ScriptStats(compilations, cacheEvictions, compilationLimitTriggered);
    }

    protected AllCircuitBreakerStats parseAllCircuitBreakerStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        final List<CircuitBreakerStats> allStats = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                long limit = 0;
                long estimated = 0;
                double overhead = 0;
                long trippedCount = 0;
                final String name = fieldName;
                while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        if ("limit_size_in_bytes".equals(fieldName)) {
                            limit = parser.longValue();
                        } else if ("estimated_size_in_bytes".equals(fieldName)) {
                            estimated = parser.longValue();
                        } else if ("overhead".equals(fieldName)) {
                            overhead = parser.doubleValue();
                        } else if ("tripped".equals(fieldName)) {
                            trippedCount = parser.longValue();
                        }
                    }
                    parser.nextToken();
                }
                allStats.add(new CircuitBreakerStats(name, limit, estimated, overhead, trippedCount));
            }
            parser.nextToken();
        }
        return new AllCircuitBreakerStats(allStats.toArray(new CircuitBreakerStats[allStats.size()]));
    }

    protected HttpStats parseHttpStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long serverOpen = 0;
        long totalOpened = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("current_open".equals(fieldName)) {
                    serverOpen = parser.longValue();
                } else if ("total_opened".equals(fieldName)) {
                    totalOpened = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new HttpStats(serverOpen, totalOpened);
    }

    protected TransportStats parseTransportStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long serverOpen = 0;
        long totalOutboundConnections = 0;
        long rxCount = 0;
        long rxSize = 0;
        long txCount = 0;
        long txSize = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("server_open".equals(fieldName)) {
                    serverOpen = parser.longValue();
                } else if ("total_outbound_connections".equals(fieldName)) {
                    totalOutboundConnections = parser.longValue();
                } else if ("rx_count".equals(fieldName)) {
                    rxCount = parser.longValue();
                } else if ("rx_size_in_bytes".equals(fieldName)) {
                    rxSize = parser.longValue();
                } else if ("tx_count".equals(fieldName)) {
                    txCount = parser.longValue();
                } else if ("tx_size_in_bytes".equals(fieldName)) {
                    txSize = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new TransportStats(serverOpen, totalOutboundConnections, rxCount, rxSize, txCount, txSize);
    }

    protected FsInfo parseFsInfo(final XContentParser parser) throws IOException {
        String fieldName = null;
        long timestamp = 0;
        final FsInfo.IoStats ioStats = null;
        final List<FsInfo.Path> paths = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("data".equals(fieldName)) {
                    parser.nextToken();
                    while ((token = parser.currentToken()) != XContentParser.Token.END_ARRAY) {
                        String path = null;
                        String mount = null;
                        long total = 0;
                        long available = 0;
                        long free = 0;
                        parser.nextToken();
                        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                fieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("total_in_bytes".equals(fieldName)) {
                                    total = parser.longValue();
                                } else if ("available_in_bytes".equals(fieldName)) {
                                    available = parser.longValue();
                                } else if ("free_in_bytes".equals(fieldName)) {
                                    free = parser.longValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("path".equals(fieldName)) {
                                    path = parser.text();
                                } else if ("mount".equals(fieldName)) {
                                    mount = parser.text();
                                }
                            }
                            parser.nextToken();
                        }
                        paths.add(new FsInfo.Path(path, mount, total, free, available));
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "timestamp".equals(fieldName)) {
                timestamp = parser.longValue();
            }
            parser.nextToken();
        }
        return new FsInfo(timestamp, ioStats, paths.toArray(new FsInfo.Path[paths.size()]));
    }

    protected DiskUsage parseFsInfoIskUsage(final XContentParser parser) throws IOException {
        String fieldName = null;
        final String nodeId = "";
        final String nodeName = "";
        String path = null;
        long totalBytes = 0;
        long freeBytes = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total_in_bytes".equals(fieldName)) {
                    totalBytes = parser.longValue();
                } else if ("available_in_bytes".equals(fieldName)) {
                    freeBytes = parser.longValue();
                }
            } else if ((token == XContentParser.Token.VALUE_STRING) && "path".equals(fieldName)) {
                path = parser.text();
            }
            parser.nextToken();
        }
        return new DiskUsage(nodeId, nodeName, path, totalBytes, freeBytes);
    }

    protected ThreadPoolStats parseThreadPoolStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        final List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                int threads = 0;
                int queue = 0;
                int active = 0;
                long rejected = 0;
                int largest = 0;
                long completed = 0;
                long waitTimeNanos = 0;
                final String name = fieldName;
                while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        if ("threads".equals(fieldName)) {
                            threads = parser.intValue();
                        } else if ("queue".equals(fieldName)) {
                            queue = parser.intValue();
                        } else if ("active".equals(fieldName)) {
                            active = parser.intValue();
                        } else if ("rejected".equals(fieldName)) {
                            rejected = parser.longValue();
                        } else if ("largest".equals(fieldName)) {
                            largest = parser.intValue();
                        } else if ("completed".equals(fieldName)) {
                            completed = parser.longValue();
                        } else if ("total_wait_time_in_nanos".equals(fieldName)) {
                            waitTimeNanos = parser.longValue();
                        }
                    }
                    parser.nextToken();
                }
                stats.add(new ThreadPoolStats.Stats(name, threads, queue, active, rejected, largest, completed, waitTimeNanos));
            }
            parser.nextToken();
        }
        return new ThreadPoolStats(stats);
    }

    protected JvmStats parseJvmStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long timestamp = 0;
        long uptime = 0;
        JvmStats.Mem mem = null;
        JvmStats.Threads threads = null;
        JvmStats.GarbageCollectors gc = null;
        List<JvmStats.BufferPool> bufferPools = null;
        JvmStats.Classes classes = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("mem".equals(fieldName)) {
                    mem = parseJvmStatsMem(parser);
                } else if ("threads".equals(fieldName)) {
                    threads = parseJvmStatsThreads(parser);
                } else if ("gc".equals(fieldName)) {
                    gc = parseJvmStatsGc(parser);
                } else if ("buffer_pools".equals(fieldName)) {
                    bufferPools = parseJvmStatsBufferPools(parser);
                } else if ("classes".equals(fieldName)) {
                    classes = parseJvmStatsClasses(parser);
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                } else if ("uptime_in_millis".equals(fieldName)) {
                    uptime = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new JvmStats(timestamp, uptime, mem, threads, gc, bufferPools, classes);
    }

    protected JvmStats.Classes parseJvmStatsClasses(final XContentParser parser) throws IOException {
        String fieldName = null;
        long loadedClassCount = 0;
        long totalLoadedClassCount = 0;
        long unloadedClassCount = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("current_loaded_count".equals(fieldName)) {
                    loadedClassCount = parser.longValue();
                } else if ("used_in_bytes".equals(fieldName)) {
                    totalLoadedClassCount = parser.longValue();
                } else if ("total_unloaded_count".equals(fieldName)) {
                    unloadedClassCount = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new JvmStats.Classes(loadedClassCount, totalLoadedClassCount, unloadedClassCount);
    }

    protected List<JvmStats.BufferPool> parseJvmStatsBufferPools(final XContentParser parser) throws IOException {
        String fieldName = null;
        final List<JvmStats.BufferPool> bufferPools = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                long count = 0;
                long totalCapacity = 0;
                long used = 0;
                final String name = fieldName;
                while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        if ("count".equals(fieldName)) {
                            count = parser.longValue();
                        } else if ("total_capacity_in_bytes".equals(fieldName)) {
                            totalCapacity = parser.longValue();
                        } else if ("used_in_bytes".equals(fieldName)) {
                            used = parser.longValue();
                        }
                    }
                    parser.nextToken();
                }
                bufferPools.add(new JvmStats.BufferPool(name, count, totalCapacity, used));
            }
            parser.nextToken();
        }
        return bufferPools;
    }

    protected JvmStats.GarbageCollectors parseJvmStatsGc(final XContentParser parser) throws IOException {
        String fieldName = null;
        final List<JvmStats.GarbageCollector> collectors = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("collectors".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            parser.nextToken();
                            long collectionCount = 0;
                            long collectionTime = 0;
                            final String name = fieldName;
                            while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                    if ("collection_count".equals(fieldName)) {
                                        collectionCount = parser.longValue();
                                    } else if ("collection_time_in_millis".equals(fieldName)) {
                                        collectionTime = parser.longValue();
                                    }
                                }
                                parser.nextToken();
                            }
                            collectors.add(new JvmStats.GarbageCollector(name, collectionCount, collectionTime));
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            }
            parser.nextToken();
        }
        return new JvmStats.GarbageCollectors(collectors.toArray(new JvmStats.GarbageCollector[collectors.size()]));
    }

    protected JvmStats.Threads parseJvmStatsThreads(final XContentParser parser) throws IOException {
        String fieldName = null;
        int count = 0;
        int peakCount = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("count".equals(fieldName)) {
                    count = parser.intValue();
                } else if ("peak_count".equals(fieldName)) {
                    peakCount = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return new JvmStats.Threads(count, peakCount);
    }

    protected JvmStats.Mem parseJvmStatsMem(final XContentParser parser) throws IOException {
        String fieldName = null;
        long heapCommitted = 0;
        long heapUsed = 0;
        long heapMax = 0;
        long nonHeapCommitted = 0;
        long nonHeapUsed = 0;
        final List<JvmStats.MemoryPool> pools = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("pools".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            parser.nextToken();
                            long used = 0;
                            long max = 0;
                            long peakUsed = 0;
                            long peakMax = 0;
                            MemoryPoolGcStats lastGcStats = null;
                            final String name = fieldName;
                            while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                    if ("used_in_bytes".equals(fieldName)) {
                                        used = parser.longValue();
                                    } else if ("max_in_bytes".equals(fieldName)) {
                                        max = parser.longValue();
                                    } else if ("peak_used_in_bytes".equals(fieldName)) {
                                        peakUsed = parser.longValue();
                                    } else if ("peak_max_in_bytes".equals(fieldName)) {
                                        peakMax = parser.longValue();
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if ("last_gc_stats".equals(fieldName)) {
                                        parser.nextToken();
                                        long lastUsed = 0;
                                        long lastMax = 0;
                                        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                                            if (token == XContentParser.Token.FIELD_NAME) {
                                                fieldName = parser.currentName();
                                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                                if ("used_in_bytes".equals(fieldName)) {
                                                    lastUsed = parser.longValue();
                                                } else if ("max_in_bytes".equals(fieldName)) {
                                                    lastMax = parser.longValue();
                                                }
                                            }
                                            parser.nextToken();
                                        }
                                        lastGcStats = new MemoryPoolGcStats(lastUsed, lastMax);
                                    } else {
                                        consumeObject(parser);
                                    }
                                }
                                parser.nextToken();
                            }
                            pools.add(new JvmStats.MemoryPool(name, used, max, peakUsed, peakMax,
                                    lastGcStats != null ? lastGcStats : new MemoryPoolGcStats(0, 0)));
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("heap_committed_in_bytes".equals(fieldName)) {
                    heapCommitted = parser.longValue();
                } else if ("heap_used_in_bytes".equals(fieldName)) {
                    heapUsed = parser.longValue();
                } else if ("heap_max_in_bytes".equals(fieldName)) {
                    heapMax = parser.longValue();
                } else if ("non_heap_committed_in_bytes".equals(fieldName)) {
                    nonHeapCommitted = parser.longValue();
                } else if ("non_heap_used_in_bytes".equals(fieldName)) {
                    nonHeapUsed = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new JvmStats.Mem(heapCommitted, heapUsed, heapMax, nonHeapCommitted, nonHeapUsed, pools);
    }

    protected ProcessStats parseProcessStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long timestamp = 0;
        long openFileDescriptors = 0;
        long maxFileDescriptors = 0;
        XContentParser.Token token;
        ProcessStats.Cpu cpu = null;
        ProcessStats.Mem mem = null;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("cpu".equals(fieldName)) {
                    cpu = parseProcessStatsCpu(parser);
                } else if ("mem".equals(fieldName)) {
                    mem = parseProcessStatsMem(parser);
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                } else if ("open_file_descriptors".equals(fieldName)) {
                    openFileDescriptors = parser.longValue();
                } else if ("max_file_descriptors".equals(fieldName)) {
                    maxFileDescriptors = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new ProcessStats(timestamp, openFileDescriptors, maxFileDescriptors, cpu, mem);
    }

    protected ProcessStats.Mem parseProcessStatsMem(final XContentParser parser) throws IOException {
        String fieldName = null;
        long totalVirtual = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "total_virtual_in_bytes".equals(fieldName)) {
                totalVirtual = parser.longValue();
            }
            parser.nextToken();
        }
        return new ProcessStats.Mem(totalVirtual);
    }

    protected ProcessStats.Cpu parseProcessStatsCpu(final XContentParser parser) throws IOException {
        String fieldName = null;
        short percent = 0;
        long total = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("percent".equals(fieldName)) {
                    percent = parser.shortValue();
                } else if ("total_in_millis".equals(fieldName)) {
                    total = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return new ProcessStats.Cpu(percent, total);
    }

    protected OsStats parseOsStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long timestamp = 0;
        OsStats.Cpu cpu = null;
        OsStats.Mem mem = null;
        OsStats.Swap swap = null;
        OsStats.Cgroup cgroup = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("cpu".equals(fieldName)) {
                    cpu = parseOsStatsCpu(parser);
                } else if ("mem".equals(fieldName)) {
                    mem = parseOsStatsMem(parser);
                } else if ("swap".equals(fieldName)) {
                    swap = parseOsStatsSwap(parser);
                } else if ("cgroup".equals(fieldName)) {
                    cgroup = parseOsStatsCgroup(parser);
                } else {
                    consumeObject(parser);
                }
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "timestamp".equals(fieldName)) {
                timestamp = parser.longValue();
            }
            parser.nextToken();
        }
        return new OsStats(timestamp, cpu, mem, swap, cgroup);
    }

    protected OsStats.Cgroup parseOsStatsCgroup(final XContentParser parser) throws IOException {
        String fieldName = null;
        String cpuAcctControlGroup = null;
        long cpuAcctUsageNanos = 0;
        String cpuControlGroup = null;
        long cpuCfsPeriodMicros = 0;
        long cpuCfsQuotaMicros = 0;
        OsStats.Cgroup.CpuStat cpuStat = null;
        String memoryControlGroup = null;
        String memoryLimitInBytes = null;
        String memoryUsageInBytes = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("cpuacct".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("control_group".equals(fieldName)) {
                                cpuAcctControlGroup = parser.text();
                            }
                        } else if ((token == XContentParser.Token.VALUE_NUMBER) && "usage_nanos".equals(fieldName)) {
                            cpuAcctUsageNanos = parser.longValue();
                        }
                        parser.nextToken();
                    }
                } else if ("cpu".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("control_group".equals(fieldName)) {
                                cpuControlGroup = parser.text();
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("cfs_period_micros".equals(fieldName)) {
                                cpuCfsPeriodMicros = parser.longValue();
                            } else if ("cfs_quota_micros".equals(fieldName)) {
                                cpuCfsQuotaMicros = parser.longValue();
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            parser.nextToken();
                            if ("stat".equals(fieldName)) {
                                long numberOfElapsedPeriods = 0;
                                long numberOfTimesThrottled = 0;
                                long timeThrottledNanos = 0;
                                while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        fieldName = parser.currentName();
                                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                        if ("number_of_elapsed_periods".equals(fieldName)) {
                                            numberOfElapsedPeriods = parser.longValue();
                                        } else if ("number_of_times_throttled".equals(fieldName)) {
                                            numberOfTimesThrottled = parser.longValue();
                                        } else if ("time_throttled_nanos".equals(fieldName)) {
                                            timeThrottledNanos = parser.longValue();
                                        }
                                    }
                                    parser.nextToken();
                                }
                                cpuStat = new OsStats.Cgroup.CpuStat(numberOfElapsedPeriods, numberOfTimesThrottled, timeThrottledNanos);
                            } else {
                                consumeObject(parser);
                            }
                        }
                        parser.nextToken();
                    }
                } else if ("memory".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("control_group".equals(fieldName)) {
                                memoryControlGroup = parser.text();
                            } else if ("limit_in_bytes".equals(fieldName)) {
                                memoryLimitInBytes = parser.text();
                            } else if ("usage_in_bytes".equals(fieldName)) {
                                memoryUsageInBytes = parser.text();
                            }
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            }
            parser.nextToken();
        }
        return new OsStats.Cgroup(cpuAcctControlGroup, cpuAcctUsageNanos, cpuControlGroup, cpuCfsPeriodMicros, cpuCfsQuotaMicros, cpuStat,
                memoryControlGroup, memoryLimitInBytes, memoryUsageInBytes);
    }

    protected OsStats.Swap parseOsStatsSwap(final XContentParser parser) throws IOException {
        String fieldName = null;
        long total = 0;
        long free = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total_in_bytes".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("free_in_bytes".equals(fieldName)) {
                    free = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new OsStats.Swap(total, free);
    }

    protected OsStats.Mem parseOsStatsMem(final XContentParser parser) throws IOException {
        String fieldName = null;
        long total = 0;
        long free = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total_in_bytes".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("free_in_bytes".equals(fieldName)) {
                    free = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new OsStats.Mem(total, free);
    }

    protected OsStats.Cpu parseOsStatsCpu(final XContentParser parser) throws IOException {
        String fieldName = null;
        short systemCpuPercent = 0;
        double[] systemLoadAverage = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("load_average".equals(fieldName)) {
                    final Map<String, Double> values = new HashMap<>();
                    String key = null;
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            values.put(key, parser.doubleValue());
                        }
                        parser.nextToken();
                    }
                    systemLoadAverage = new double[3];
                    systemLoadAverage[0] = values.containsKey("1m") ? values.get("1m") : -1;
                    systemLoadAverage[1] = values.containsKey("5m") ? values.get("5m") : -1;
                    systemLoadAverage[2] = values.containsKey("15m") ? values.get("15m") : -1;
                } else {
                    consumeObject(parser);
                }
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "percent".equals(fieldName)) {
                systemCpuPercent = parser.shortValue();
            }
            parser.nextToken();
        }
        return new OsStats.Cpu(systemCpuPercent, systemLoadAverage);
    }

    protected NodeIndicesStats parseNodeIndicesStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        DocsStats docs = null;
        StoreStats store = null;
        IndexingStats indexing = null;
        GetStats get = null;
        SearchStats search = null;
        MergeStats merge = null;
        RefreshStats refresh = null;
        FlushStats flush = null;
        WarmerStats warmer = null;
        QueryCacheStats queryCache = null;
        FieldDataStats fieldData = null;
        CompletionStats completion = null;
        SegmentsStats segments = null;
        TranslogStats translog = null;
        RequestCacheStats requestCache = null;
        RecoveryStats recoveryStats = null;
        SearchRequestStats searchRequestStats = new SearchRequestStats(clusterSettings);
        final Map<Index, List<IndexShardStats>> statsByShard = Collections.emptyMap();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("docs".equals(fieldName)) {
                    docs = parseDocsStats(parser);
                } else if ("store".equals(fieldName)) {
                    store = parseStoreStats(parser);
                } else if ("indexing".equals(fieldName)) {
                    indexing = parseIndexingStats(parser);
                } else if ("get".equals(fieldName)) {
                    get = parseGetStats(parser);
                } else if ("search".equals(fieldName)) {
                    search = parseSearchStats(parser);
                } else if ("merge".equals(fieldName)) {
                    merge = parseMergeStats(parser);
                } else if ("refresh".equals(fieldName)) {
                    refresh = parseRefreshStats(parser);
                } else if ("flush".equals(fieldName)) {
                    flush = parseFlushStats(parser);
                } else if ("warmer".equals(fieldName)) {
                    warmer = parseWarmerStats(parser);
                } else if ("query_cache".equals(fieldName)) {
                    queryCache = parseQueryCacheStats(parser);
                } else if ("fielddata".equals(fieldName)) {
                    fieldData = parseFieldDataStats(parser);
                } else if ("completion".equals(fieldName)) {
                    completion = parseCompletionStats(parser);
                } else if ("segments".equals(fieldName)) {
                    segments = parseSegmentsStats(parser);
                } else if ("translog".equals(fieldName)) {
                    translog = parseTranslogStats(parser);
                } else if ("request_cache".equals(fieldName)) {
                    requestCache = parseRequestCacheStats(parser);
                } else if ("recovery".equals(fieldName)) {
                    recoveryStats = parseRecoveryStats(parser);
                } else {
                    consumeObject(parser);
                }

            }
            parser.nextToken();
        }
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeOptionalWriteable(docs);
            out.writeOptionalWriteable(store);
            out.writeOptionalWriteable(indexing);
            out.writeOptionalWriteable(get);
            out.writeOptionalWriteable(search);
            out.writeOptionalWriteable(merge);
            out.writeOptionalWriteable(refresh);
            out.writeOptionalWriteable(flush);
            out.writeOptionalWriteable(warmer);
            out.writeOptionalWriteable(queryCache);
            out.writeOptionalWriteable(fieldData);
            out.writeOptionalWriteable(completion);
            out.writeOptionalWriteable(segments);
            out.writeOptionalWriteable(translog);
            out.writeOptionalWriteable(requestCache);
            out.writeOptionalWriteable(recoveryStats);
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray()))) {
                return new NodeIndicesStats(new CommonStats(in), statsByShard, searchRequestStats);
            }
        }
    }

    protected RecoveryStats parseRecoveryStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        int currentAsSource = 0;
        int currentAsTarget = 0;
        long throttleTimeInNanos = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("current_as_source".equals(fieldName)) {
                    currentAsSource = parser.intValue();
                } else if ("current_as_target".equals(fieldName)) {
                    currentAsTarget = parser.intValue();
                } else if ("throttle_time_in_millis".equals(fieldName)) {
                    throttleTimeInNanos = parser.longValue();
                }
            }
            parser.nextToken();
        }
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(currentAsSource);
            out.writeVInt(currentAsTarget);
            out.writeLong(throttleTimeInNanos);
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray()))) {
                return new RecoveryStats(in);
            }
        }
    }

    protected RequestCacheStats parseRequestCacheStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long memorySize = 0;
        long evictions = 0;
        long hitCount = 0;
        long missCount = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("memory_size_in_bytes".equals(fieldName)) {
                    memorySize = parser.longValue();
                } else if ("evictions".equals(fieldName)) {
                    evictions = parser.longValue();
                } else if ("hit_count".equals(fieldName)) {
                    hitCount = parser.longValue();
                } else if ("miss_count".equals(fieldName)) {
                    missCount = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new RequestCacheStats(memorySize, evictions, hitCount, missCount);
    }

    protected TranslogStats parseTranslogStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        int numberOfOperations = 0;
        long translogSizeInBytes = 0;
        int uncommittedOperations = 0;
        long uncommittedSizeInBytes = 0;
        long earliestLastModifiedAge = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("operations".equals(fieldName)) {
                    numberOfOperations = parser.intValue();
                } else if ("size_in_bytes".equals(fieldName)) {
                    translogSizeInBytes = parser.longValue();
                } else if ("uncommitted_operations".equals(fieldName)) {
                    uncommittedOperations = parser.intValue();
                } else if ("uncommitted_size_in_bytes".equals(fieldName)) {
                    uncommittedSizeInBytes = parser.longValue();
                } else if ("earliest_last_modified_age".equals(fieldName)) {
                    earliestLastModifiedAge = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new TranslogStats(numberOfOperations, translogSizeInBytes, uncommittedOperations, uncommittedSizeInBytes,
                earliestLastModifiedAge);
    }

    protected SegmentsStats parseSegmentsStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long count = 0;
        long memoryInBytes = 0;
        long termsMemoryInBytes = 0;
        long storedFieldsMemoryInBytes = 0;
        long termVectorsMemoryInBytes = 0;
        long normsMemoryInBytes = 0;
        long pointsMemoryInBytes = 0;
        long docValuesMemoryInBytes = 0;
        long indexWriterMemoryInBytes = 0;
        long versionMapMemoryInBytes = 0;
        long bitsetMemoryInBytes = 0;
        long maxUnsafeAutoIdTimestamp = 0;
        final Map<String, Long> fileSizes = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                if ("file_sizes".equals(fieldName)) {
                    String key = null;
                    while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            fileSizes.put(key, parser.longValue());
                        }
                        parser.nextToken();
                    }
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("count".equals(fieldName)) {
                    count = parser.longValue();
                } else if ("memory_in_bytes".equals(fieldName)) {
                    memoryInBytes = parser.longValue();
                } else if ("terms_memory_in_bytes".equals(fieldName)) {
                    termsMemoryInBytes = parser.longValue();
                } else if ("stored_fields_memory_in_bytes".equals(fieldName)) {
                    storedFieldsMemoryInBytes = parser.longValue();
                } else if ("term_vectors_memory_in_bytes".equals(fieldName)) {
                    termVectorsMemoryInBytes = parser.longValue();
                } else if ("norms_memory_in_bytes".equals(fieldName)) {
                    normsMemoryInBytes = parser.longValue();
                } else if ("points_memory_in_bytes".equals(fieldName)) {
                    pointsMemoryInBytes = parser.longValue();
                } else if ("doc_values_memory_in_bytes".equals(fieldName)) {
                    docValuesMemoryInBytes = parser.longValue();
                } else if ("index_writer_memory_in_bytes".equals(fieldName)) {
                    indexWriterMemoryInBytes = parser.longValue();
                } else if ("version_map_memory_in_bytes".equals(fieldName)) {
                    versionMapMemoryInBytes = parser.longValue();
                } else if ("fixed_bit_set_memory_in_bytes".equals(fieldName)) {
                    bitsetMemoryInBytes = parser.longValue();
                } else if ("max_unsafe_auto_id_timestamp".equals(fieldName)) {
                    maxUnsafeAutoIdTimestamp = parser.longValue();
                }
            }
            parser.nextToken();
        }
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVLong(count);
            out.writeLong(memoryInBytes);
            out.writeLong(termsMemoryInBytes);
            out.writeLong(storedFieldsMemoryInBytes);
            out.writeLong(termVectorsMemoryInBytes);
            out.writeLong(normsMemoryInBytes);
            out.writeLong(pointsMemoryInBytes);
            out.writeLong(docValuesMemoryInBytes);
            out.writeLong(indexWriterMemoryInBytes);
            out.writeLong(versionMapMemoryInBytes);
            out.writeLong(bitsetMemoryInBytes);
            out.writeLong(maxUnsafeAutoIdTimestamp);
            out.writeVInt(fileSizes.size());
            for (final Map.Entry<String, Long> entry : fileSizes.entrySet()) {
                out.writeString(entry.getKey());
                out.writeLong(entry.getValue());
            }
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray()))) {
                return new SegmentsStats(in);
            }
        }
    }

    protected CompletionStats parseCompletionStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long size = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ((token == XContentParser.Token.VALUE_NUMBER) && "size_in_bytes".equals(fieldName)) {
                size = parser.longValue();
            }
            parser.nextToken();
        }
        return new CompletionStats(size, null);
    }

    protected FieldDataStats parseFieldDataStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long memorySize = 0;
        long evictions = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("memory_size_in_bytes".equals(fieldName)) {
                    memorySize = parser.longValue();
                } else if ("evictions".equals(fieldName)) {
                    evictions = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new FieldDataStats(memorySize, evictions, null);
    }

    protected QueryCacheStats parseQueryCacheStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long ramBytesUsed = 0;
        long hitCount = 0;
        long missCount = 0;
        long cacheCount = 0;
        long cacheSize = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("memory_size_in_bytes".equals(fieldName)) {
                    ramBytesUsed = parser.longValue();
                } else if ("hit_count".equals(fieldName)) {
                    hitCount = parser.longValue();
                } else if ("miss_count".equals(fieldName)) {
                    missCount = parser.longValue();
                } else if ("cache_count".equals(fieldName)) {
                    cacheCount = parser.longValue();
                } else if ("cache_size".equals(fieldName)) {
                    cacheSize = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new QueryCacheStats(ramBytesUsed, hitCount, missCount, cacheCount, cacheSize);
    }

    protected WarmerStats parseWarmerStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long current = 0;
        long total = 0;
        long totalTimeInMillis = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("current".equals(fieldName)) {
                    current = parser.longValue();
                } else if ("total".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("total_time_in_millis".equals(fieldName)) {
                    totalTimeInMillis = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new WarmerStats(current, total, totalTimeInMillis);
    }

    protected FlushStats parseFlushStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long total = 0;
        long periodic = 0;
        long totalTimeInMillis = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("periodic".equals(fieldName)) {
                    periodic = parser.longValue();
                } else if ("total_time_in_millis".equals(fieldName)) {
                    totalTimeInMillis = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new FlushStats(total, periodic, totalTimeInMillis);
    }

    protected RefreshStats parseRefreshStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long total = 0;
        long totalTimeInMillis = 0;
        long externalTotal = 0;
        long externalTotalTimeInMillis = 0;
        int listeners = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("total_time_in_millis".equals(fieldName)) {
                    totalTimeInMillis = parser.longValue();
                } else if ("external_total".equals(fieldName)) {
                    externalTotal = parser.longValue();
                } else if ("external_total_time_in_millis".equals(fieldName)) {
                    externalTotalTimeInMillis = parser.longValue();
                } else if ("listeners".equals(fieldName)) {
                    listeners = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return new RefreshStats(total, totalTimeInMillis, externalTotal, externalTotalTimeInMillis, listeners);
    }

    protected MergeStats parseMergeStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long total = 0;
        long totalTimeInMillis = 0;
        long totalNumDocs = 0;
        long totalSizeInBytes = 0;
        long current = 0;
        long currentNumDocs = 0;
        long currentSizeInBytes = 0;
        long totalStoppedTimeInMillis = 0;
        long totalThrottledTimeInMillis = 0;
        long totalBytesPerSecAutoThrottle = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    total = parser.longValue();
                } else if ("total_time_in_millis".equals(fieldName)) {
                    totalTimeInMillis = parser.longValue();
                } else if ("total_docs".equals(fieldName)) {
                    totalNumDocs = parser.intValue();
                } else if ("total_size_in_bytes".equals(fieldName)) {
                    totalSizeInBytes = parser.intValue();
                } else if ("current".equals(fieldName)) {
                    current = parser.intValue();
                } else if ("current_docs".equals(fieldName)) {
                    currentNumDocs = parser.intValue();
                } else if ("current_size_in_bytes".equals(fieldName)) {
                    currentSizeInBytes = parser.intValue();
                } else if ("total_stopped_time_in_millis".equals(fieldName)) {
                    totalStoppedTimeInMillis = parser.intValue();
                } else if ("total_throttled_time_in_millis".equals(fieldName)) {
                    totalThrottledTimeInMillis = parser.intValue();
                } else if ("total_auto_throttle_in_bytes".equals(fieldName)) {
                    totalBytesPerSecAutoThrottle = parser.intValue();
                }
            }
            parser.nextToken();
        }
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVLong(total);
            out.writeVLong(totalTimeInMillis);
            out.writeVLong(totalNumDocs);
            out.writeVLong(totalSizeInBytes);
            out.writeVLong(current);
            out.writeVLong(currentNumDocs);
            out.writeVLong(currentSizeInBytes);
            out.writeVLong(totalStoppedTimeInMillis);
            out.writeVLong(totalThrottledTimeInMillis);
            out.writeVLong(totalBytesPerSecAutoThrottle);
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray()))) {
                return new MergeStats(in);
            }
        }
    }

    protected SearchStats parseSearchStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long queryCount = 0;
        long queryTimeInMillis = 0;
        long queryCurrent = 0;
        long concurrentQueryCount = 0;
        long concurrentQueryTimeInMillis = 0;
        long concurrentQueryCurrent = 0;
        double concurrentAvgAliceCount = 0;
        long fetchCount = 0;
        long fetchTimeInMillis = 0;
        long fetchCurrent = 0;
        long scrollCount = 0;
        long scrollTimeInMillis = 0;
        long scrollCurrent = 0;
        long pitCount = 0;
        long pitTimeInMillis = 0;
        long pitCurrent = 0;
        long suggestCount = 0;
        long suggestTimeInMillis = 0;
        long suggestCurrent = 0;
        long openContexts = 0;
        long searchIdleReactivateCount = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("query_total".equals(fieldName)) {
                    queryCount = parser.longValue();
                } else if ("query_time_in_millis".equals(fieldName)) {
                    queryTimeInMillis = parser.longValue();
                } else if ("query_current".equals(fieldName)) {
                    queryCurrent = parser.longValue();
                } else if ("concurrent_query_total".equals(fieldName)) {
                    concurrentQueryCount = parser.longValue();
                } else if ("concurrent_query_time_in_millis".equals(fieldName)) {
                    concurrentQueryTimeInMillis = parser.longValue();
                } else if ("concurrent_query_current".equals(fieldName)) {
                    concurrentQueryCurrent = parser.longValue();
                } else if ("concurrent_avg_slice_count".equals(fieldName)) {
                    concurrentAvgAliceCount = parser.doubleValue();
                } else if ("fetch_total".equals(fieldName)) {
                    fetchCount = parser.longValue();
                } else if ("fetch_time_in_millis".equals(fieldName)) {
                    fetchTimeInMillis = parser.longValue();
                } else if ("fetch_current".equals(fieldName)) {
                    fetchCurrent = parser.longValue();
                } else if ("scroll_total".equals(fieldName)) {
                    scrollCount = parser.longValue();
                } else if ("scroll_time_in_millis".equals(fieldName)) {
                    scrollTimeInMillis = parser.longValue();
                } else if ("scroll_current".equals(fieldName)) {
                    scrollCurrent = parser.longValue();
                } else if ("point_in_time_total".equals(fieldName)) {
                    pitCount = parser.longValue();
                } else if ("point_in_time_time_in_millis".equals(fieldName)) {
                    pitTimeInMillis = parser.longValue();
                } else if ("point_in_time_current".equals(fieldName)) {
                    pitCurrent = parser.longValue();
                } else if ("suggest_total".equals(fieldName)) {
                    suggestCount = parser.longValue();
                } else if ("suggest_time_in_millis".equals(fieldName)) {
                    suggestTimeInMillis = parser.longValue();
                } else if ("suggest_current".equals(fieldName)) {
                    suggestCurrent = parser.longValue();
                } else if ("open_contexts".equals(fieldName)) {
                    openContexts = parser.longValue();
                } else if ("search_idle_reactivate_count_total".equals(fieldName)) {
                    searchIdleReactivateCount = parser.longValue();
                }
            }
            parser.nextToken();
        }
        long queryConcurrency = 0;
        if (concurrentQueryCount != 0) {
            queryConcurrency = (long) (concurrentQueryCount * concurrentAvgAliceCount);
        }
        return new SearchStats(new SearchStats.Stats(//
                queryCount, //
                queryTimeInMillis, //
                queryCurrent, //
                concurrentQueryCount, //
                concurrentQueryTimeInMillis, //
                concurrentQueryCurrent, //
                queryConcurrency, //
                fetchCount, //
                fetchTimeInMillis, //
                fetchCurrent, //
                scrollCount, //
                scrollTimeInMillis, //
                scrollCurrent, //
                pitCount, //
                pitTimeInMillis, //
                pitCurrent, //
                suggestCount, //
                suggestTimeInMillis, //
                suggestCurrent, //
                searchIdleReactivateCount), openContexts, null);
    }

    protected GetStats parseGetStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long existsCount = 0;
        long existsTimeInMillis = 0;
        long missingCount = 0;
        long missingTimeInMillis = 0;
        long current = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("exists_total".equals(fieldName)) {
                    existsCount = parser.longValue();
                } else if ("exists_time_in_millis".equals(fieldName)) {
                    existsTimeInMillis = parser.longValue();
                } else if ("missing_total".equals(fieldName)) {
                    missingCount = parser.longValue();
                } else if ("missing_time_in_millis".equals(fieldName)) {
                    missingTimeInMillis = parser.longValue();
                } else if ("current".equals(fieldName)) {
                    current = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new GetStats(existsCount, existsTimeInMillis, missingCount, missingTimeInMillis, current);
    }

    protected IndexingStats parseIndexingStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long indexCount = 0;
        long indexTimeInMillis = 0;
        long indexCurrent = 0;
        long indexFailedCount = 0;
        long deleteCount = 0;
        long deleteTimeInMillis = 0;
        long deleteCurrent = 0;
        long noopUpdateCount = 0;
        boolean isThrottled = false;
        long throttleTimeInMillis = 0;
        DocStatusStats docStatusStats = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("is_throttled".equals(fieldName)) {
                    isThrottled = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("index_total".equals(fieldName)) {
                    indexCount = parser.longValue();
                } else if ("index_time_in_millis".equals(fieldName)) {
                    indexTimeInMillis = parser.longValue();
                } else if ("index_current".equals(fieldName)) {
                    indexCurrent = parser.longValue();
                } else if ("index_failed".equals(fieldName)) {
                    indexFailedCount = parser.longValue();
                } else if ("delete_total".equals(fieldName)) {
                    deleteCount = parser.longValue();
                } else if ("delete_time_in_millis".equals(fieldName)) {
                    deleteTimeInMillis = parser.longValue();
                } else if ("delete_current".equals(fieldName)) {
                    deleteCurrent = parser.longValue();
                } else if ("noop_update_total".equals(fieldName)) {
                    noopUpdateCount = parser.longValue();
                } else if ("throttle_time_in_millis".equals(fieldName)) {
                    throttleTimeInMillis = parser.longValue();
                }
            } else if ("doc_status".equals(fieldName)) {
                consumeObject(parser);
            }
            parser.nextToken();
        }
        return new IndexingStats(new IndexingStats.Stats(indexCount, indexTimeInMillis, indexCurrent, indexFailedCount, deleteCount,
                deleteTimeInMillis, deleteCurrent, noopUpdateCount, isThrottled, throttleTimeInMillis, docStatusStats));
    }

    protected StoreStats parseStoreStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long sizeInBytes = 0;
        long reservedSize = -1L;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size_in_bytes".equals(fieldName)) {
                    sizeInBytes = parser.longValue();
                } else if ("reserved_in_bytes".equals(fieldName)) {
                    reservedSize = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new StoreStats(sizeInBytes, reservedSize);
    }

    protected DocsStats parseDocsStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        long count = 0;
        long deleted = 0;
        long totalSizeInBytes = 0;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("count".equals(fieldName)) {
                    count = parser.longValue();
                } else if ("deleted".equals(fieldName)) {
                    deleted = parser.longValue();
                } else if ("total_size_in_bytes".equals(fieldName)) {
                    totalSizeInBytes = parser.longValue();
                }
            }
            parser.nextToken();
        }
        return new DocsStats(count, deleted, totalSizeInBytes);
    }

    protected int[] parseNodeResults(final XContentParser parser) throws IOException {
        final int results[] = new int[3];
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    results[0] = parser.intValue();
                } else if ("successful".equals(fieldName)) {
                    results[1] = parser.intValue();
                } else if ("failed".equals(fieldName)) {
                    results[2] = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return results;
    }

    protected void consumeObject(final XContentParser parser) throws IOException {
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                consumeObject(parser);
            }
            parser.nextToken();
        }
    }

    protected String getMetric(final NodesStatsRequest request) {
        final Set<String> metrics = request.requestedMetrics();
        if (request.indices().anySet() && CommonStatsFlags.ALL.getFlags().length != request.indices().getFlags().length) {
            return metrics.stream().collect(Collectors.joining(",")) + "/"
                    + Arrays.stream(request.indices().getFlags()).map(Flag::getRestName).collect(Collectors.joining(","));
        }
        return metrics.stream().collect(Collectors.joining(","));
    }

    protected CurlRequest getCurlRequest(final NodesStatsRequest request) {
        // RestNodesStatsAction
        final StringBuilder buf = new StringBuilder();
        buf.append("/_nodes");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }
        buf.append("/stats");
        final String metric = getMetric(request);
        if (metric.length() > 0) {
            buf.append('/').append(metric);
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
