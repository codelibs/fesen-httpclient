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

package org.codelibs.fesen.opensearch.action.admin.cluster.stats;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.codelibs.fesen.opensearch.action.admin.indices.stats.CommonStats;
import org.codelibs.fesen.opensearch.action.admin.indices.stats.ShardStats;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.cluster.health.ClusterHealthStatus;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.index.cache.query.QueryCacheStats;
import org.codelibs.fesen.opensearch.index.engine.SegmentsStats;
import org.codelibs.fesen.opensearch.index.fielddata.FieldDataStats;
import org.codelibs.fesen.opensearch.index.shard.DocsStats;
import org.codelibs.fesen.opensearch.index.store.StoreStats;
import org.codelibs.fesen.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for obtaining cluster stats from node level
 *
 * @opensearch.internal
 */
public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private final NodeInfo nodeInfo;
    private final NodeStats nodeStats;
    private final ShardStats[] shardsStats;
    private ClusterHealthStatus clusterStatus;
    private AggregatedNodeLevelStats aggregatedNodeLevelStats;

    /**
     * Creates a new ClusterStatsNodeResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            this.shardsStats = in.readOptionalArray(ShardStats::new, ShardStats[]::new);
            this.aggregatedNodeLevelStats = in.readOptionalWriteable(AggregatedNodeLevelStats::new);
        } else {
            this.shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
        }
    }

    /**
     * Returns the node info.
     *
     * @return the node info
     */
    public NodeInfo nodeInfo() {
        return this.nodeInfo;
    }

    /**
     * Returns the node stats.
     *
     * @return the node stats
     */
    public NodeStats nodeStats() {
        return this.nodeStats;
    }

    /**
     * Cluster Health Status, only populated on cluster-manager nodes.
     *
     * @return the cluster status
     */
    @Nullable
    public ClusterHealthStatus clusterStatus() {
        return clusterStatus;
    }

    /**
     * Returns the shards stats.
     *
     * @return the shards stats
     */
    public ShardStats[] shardsStats() {
        return this.shardsStats;
    }

    /**
     * Returns the aggregated node level stats.
     *
     * @return the aggregated node level stats
     */
    public AggregatedNodeLevelStats getAggregatedNodeLevelStats() {
        return aggregatedNodeLevelStats;
    }

    /**
     * Reads the node response.
     *
     * @param in the input to read from
     * @return the node response
     * @throws IOException if an I/O error occurs
     */
    public static ClusterStatsNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (clusterStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(clusterStatus.value());
        }
        nodeInfo.writeTo(out);
        nodeStats.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            if (aggregatedNodeLevelStats != null) {
                out.writeOptionalArray(null);
                out.writeOptionalWriteable(aggregatedNodeLevelStats);
            } else {
                out.writeOptionalArray(shardsStats);
                out.writeOptionalWriteable(null);
            }
        } else {
            out.writeArray(shardsStats);
        }
    }

    /**
     * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
     */
    public class AggregatedNodeLevelStats extends BaseNodeResponse {

        CommonStats commonStats;
        Map<String, AggregatedIndexStats> indexStatsMap;

        /**
         * Creates a new AggregatedNodeLevelStats by reading it from the given input.
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        protected AggregatedNodeLevelStats(StreamInput in) throws IOException {
            super(in);
            commonStats = in.readOptionalWriteable(CommonStats::new);
            indexStatsMap = in.readMap(StreamInput::readString, AggregatedIndexStats::new);
        }

        /**
         * Creates a new AggregatedNodeLevelStats.
         *
         * @param node the node
         * @param indexShardsStats the index shards stats
         */
        protected AggregatedNodeLevelStats(DiscoveryNode node, ShardStats[] indexShardsStats) {
            super(node);
            this.commonStats = new CommonStats();
            this.commonStats.docs = new DocsStats();
            this.commonStats.store = new StoreStats();
            this.commonStats.fieldData = new FieldDataStats();
            this.commonStats.queryCache = new QueryCacheStats();
            this.commonStats.completion = new CompletionStats();
            this.commonStats.segments = new SegmentsStats();
            this.indexStatsMap = new HashMap<>();

            // Index Level Stats
            for (org.codelibs.fesen.opensearch.action.admin.indices.stats.ShardStats shardStats : indexShardsStats) {
                AggregatedIndexStats indexShardStats = this.indexStatsMap.get(shardStats.getShardRouting().getIndexName());
                if (indexShardStats == null) {
                    indexShardStats = new AggregatedIndexStats();
                    this.indexStatsMap.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
                }

                indexShardStats.total++;

                CommonStats shardCommonStats = shardStats.getStats();

                if (shardStats.getShardRouting().primary()) {
                    indexShardStats.primaries++;
                    this.commonStats.docs.add(shardCommonStats.docs);
                }
                this.commonStats.store.add(shardCommonStats.store);
                this.commonStats.fieldData.add(shardCommonStats.fieldData);
                this.commonStats.queryCache.add(shardCommonStats.queryCache);
                this.commonStats.completion.add(shardCommonStats.completion);
                this.commonStats.segments.add(shardCommonStats.segments);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(commonStats);
            out.writeMap(indexStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }

    /**
     * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
     */
    @PublicApi(since = "2.16.0")
    public static class AggregatedIndexStats implements Writeable {
        /**
         * The total.
         */
        public int total = 0;
        /**
         * The primaries.
         */
        public int primaries = 0;

        /**
         * Creates a new AggregatedIndexStats by reading it from the given input.
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        public AggregatedIndexStats(StreamInput in) throws IOException {
            total = in.readVInt();
            primaries = in.readVInt();
        }

        /**
         * Creates a new AggregatedIndexStats.
         */
        public AggregatedIndexStats() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(primaries);
        }
    }
}
