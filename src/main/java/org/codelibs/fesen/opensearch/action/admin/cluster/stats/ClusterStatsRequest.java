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
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A request to get cluster level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private final Set<Metric> requestedMetrics = new HashSet<>();
    private final Set<IndexMetric> indexMetricsRequested = new HashSet<>();
    private Boolean computeAllMetrics = true;

    private Boolean useAggregatedNodeLevelResponses = false;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     *
     * @param nodesIds the nodes identifiers
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            out.writeOptionalBoolean(useAggregatedNodeLevelResponses);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalBoolean(computeAllMetrics);
            long longMetricFlags = 0;
            for (Metric metric : requestedMetrics) {
                longMetricFlags |= (1 << metric.getIndex());
            }
            out.writeLong(longMetricFlags);
            long longIndexMetricFlags = 0;
            for (IndexMetric indexMetric : indexMetricsRequested) {
                longIndexMetricFlags |= (1 << indexMetric.getIndex());
            }
            out.writeLong(longIndexMetricFlags);
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the cluster stats endpoint.
     */
    @PublicApi(since = "2.18.0")
    public enum Metric {
        /**
         * The OS value.
         */
        OS("os", 0),
        /**
         * The JVM value.
         */
        JVM("jvm", 1),
        /**
         * The FS value.
         */
        FS("fs", 2),
        /**
         * The PROCESS value.
         */
        PROCESS("process", 3),
        /**
         * The INGEST value.
         */
        INGEST("ingest", 4),
        /**
         * The PLUGINS value.
         */
        PLUGINS("plugins", 5),
        /**
         * The NETWORK_TYPES value.
         */
        NETWORK_TYPES("network_types", 6),
        /**
         * The DISCOVERY_TYPES value.
         */
        DISCOVERY_TYPES("discovery_types", 7),
        /**
         * The PACKAGING_TYPES value.
         */
        PACKAGING_TYPES("packaging_types", 8),
        /**
         * The INDICES value.
         */
        INDICES("indices", 9);

        private String metricName;

        private int index;

        Metric(String name, int index) {
            this.metricName = name;
            this.index = index;
        }

        /**
         * Returns the index.
         *
         * @return the index
         */
        public int getIndex() {
            return index;
        }

    }

    /**
     * An enumeration of the "core" sections of indices metrics that may be requested
     * from the cluster stats endpoint.
     *
     * When no value is provided for param index_metric, default filter is set to _all.
     */
    @PublicApi(since = "2.18.0")
    public enum IndexMetric {
        // Metrics computed from ShardStats
        /**
         * The SHARDS value.
         */
        SHARDS("shards", 0),
        /**
         * The DOCS value.
         */
        DOCS("docs", 1),
        /**
         * The STORE value.
         */
        STORE("store", 2),
        /**
         * The FIELDDATA value.
         */
        FIELDDATA("fielddata", 3),
        /**
         * The QUERY_CACHE value.
         */
        QUERY_CACHE("query_cache", 4),
        /**
         * The COMPLETION value.
         */
        COMPLETION("completion", 5),
        /**
         * The SEGMENTS value.
         */
        SEGMENTS("segments", 6),
        // Metrics computed from ClusterState
        /**
         * The ANALYSIS value.
         */
        ANALYSIS("analysis", 7),
        /**
         * The MAPPINGS value.
         */
        MAPPINGS("mappings", 8);

        private String metricName;

        private int index;

        IndexMetric(String name, int index) {
            this.metricName = name;
            this.index = index;
        }

        /**
         * Returns the index.
         *
         * @return the index
         */
        public int getIndex() {
            return this.index;
        }
    }
}
