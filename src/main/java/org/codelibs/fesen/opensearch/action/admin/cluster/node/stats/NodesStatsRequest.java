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
import org.codelibs.fesen.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

    private CommonStatsFlags indices = new CommonStatsFlags();
    private final Set<String> requestedMetrics = new HashSet<>();
    private boolean fileCacheDetailed = false;

    /**
     * Creates a new NodesStatsRequest.
     */
    public NodesStatsRequest() {
        super((String[]) null);
    }

    /**
     * Sets all the request flags.
     *
     * @return the all
     */
    public NodesStatsRequest all() {
        this.indices.all();
        this.requestedMetrics.addAll(Metric.allMetrics());
        return this;
    }

    /**
     * Get indices. Handles separately from other metrics because it may or
     * may not have submetrics.
     * @return flags indicating which indices stats to return
     */
    public CommonStatsFlags indices() {
        return indices;
    }

    /**
     * Get the names of requested metrics, excluding indices, which are
     * handled separately.
     *
     * @return the requested metrics
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    /**
     * Add metric
     *
     * @param metric the metric
     * @return this instance
     */
    public NodesStatsRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Add an array of metric names
     *
     * @param metrics the metrics
     * @return this instance
     */
    public NodesStatsRequest addMetrics(String... metrics) {
        // use sorted set for reliable ordering in error messages
        SortedSet<String> metricsSet = new TreeSet<>(Arrays.asList(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        requestedMetrics.addAll(metricsSet);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        indices.writeTo(out);
        out.writeStringArray(requestedMetrics.toArray(new String[0]));
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeBoolean(fileCacheDetailed);
        }
    }

    /**
     * Returns the file cache detailed flag.
     *
     * @return the file cache detailed flag
     */
    public boolean isFileCacheDetailed() {
        return fileCacheDetailed;
    }

    /**
     * Returns the file cache detailed.
     *
     * @param detailed the detailed
     * @return the file cache detailed
     */
    public NodesStatsRequest fileCacheDetailed(boolean detailed) {
        this.fileCacheDetailed = detailed;
        return this;
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes stats endpoint. Eventually this list will be pluggable.
     */
    public enum Metric {
        /**
         * The OS value.
         */
        OS("os"),
        /**
         * The PROCESS value.
         */
        PROCESS("process"),
        /**
         * The JVM value.
         */
        JVM("jvm"),
        /**
         * The THREAD_POOL value.
         */
        THREAD_POOL("thread_pool"),
        /**
         * The FS value.
         */
        FS("fs"),
        /**
         * The TRANSPORT value.
         */
        TRANSPORT("transport"),
        /**
         * The HTTP value.
         */
        HTTP("http"),
        /**
         * The BREAKER value.
         */
        BREAKER("breaker"),
        /**
         * The SCRIPT value.
         */
        SCRIPT("script"),
        /**
         * The DISCOVERY value.
         */
        DISCOVERY("discovery"),
        /**
         * The INGEST value.
         */
        INGEST("ingest"),
        /**
         * The ADAPTIVE_SELECTION value.
         */
        ADAPTIVE_SELECTION("adaptive_selection"),
        /**
         * The SCRIPT_CACHE value.
         */
        SCRIPT_CACHE("script_cache"),
        /**
         * The INDEXING_PRESSURE value.
         */
        INDEXING_PRESSURE("indexing_pressure"),
        /**
         * The SHARD_INDEXING_PRESSURE value.
         */
        SHARD_INDEXING_PRESSURE("shard_indexing_pressure"),
        /**
         * The SEARCH_BACKPRESSURE value.
         */
        SEARCH_BACKPRESSURE("search_backpressure"),
        /**
         * The CLUSTER_MANAGER_THROTTLING value.
         */
        CLUSTER_MANAGER_THROTTLING("cluster_manager_throttling"),
        /**
         * The WEIGHTED_ROUTING_STATS value.
         */
        WEIGHTED_ROUTING_STATS("weighted_routing"),
        /**
         * The FILE_CACHE_STATS value.
         */
        FILE_CACHE_STATS("file_cache"),
        /**
         * The TASK_CANCELLATION value.
         */
        TASK_CANCELLATION("task_cancellation"),
        /**
         * The SEARCH_PIPELINE value.
         */
        SEARCH_PIPELINE("search_pipeline"),
        /**
         * The RESOURCE_USAGE_STATS value.
         */
        RESOURCE_USAGE_STATS("resource_usage_stats"),
        /**
         * The SEGMENT_REPLICATION_BACKPRESSURE value.
         */
        SEGMENT_REPLICATION_BACKPRESSURE("segment_replication_backpressure"),
        /**
         * The REPOSITORIES value.
         */
        REPOSITORIES("repositories"),
        /**
         * The ADMISSION_CONTROL value.
         */
        ADMISSION_CONTROL("admission_control"),
        /**
         * The CACHE_STATS value.
         */
        CACHE_STATS("caches"),
        /**
         * The REMOTE_STORE value.
         */
        REMOTE_STORE("remote_store"),
        /** @deprecated Use {@link #NATIVE_MEMORY} instead. */
        @Deprecated
        NATIVE_ALLOCATOR("native_allocator"),
        /**
         * The NATIVE_MEMORY value.
         */
        NATIVE_MEMORY("native_memory");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        /**
         * Returns the metric name.
         *
         * @return the metric name
         */
        public String metricName() {
            return this.metricName;
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}
