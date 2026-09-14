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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.info;

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
 * A request to get node (cluster) level information.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private Set<String> requestedMetrics = Metric.defaultMetrics();

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     *
     * @param nodesIds the nodes identifiers
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
        defaultMetrics();
    }

    /**
     * Sets to return data for default metrics only.
     * See {@link Metric}
     * See {@link Metric#defaultMetrics()}.
     *
     * @return the default metrics
     */
    public NodesInfoRequest defaultMetrics() {
        requestedMetrics.addAll(Metric.defaultMetrics());
        return this;
    }

    /**
     * Get the names of requested metrics
     *
     * @return the requested metrics
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(requestedMetrics.toArray(new String[0]));
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list will be
     * pluggable.
     */
    public enum Metric {
        /**
         * The SETTINGS value.
         */
        SETTINGS("settings"),
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
         * The TRANSPORT value.
         */
        TRANSPORT("transport"),
        /**
         * The HTTP value.
         */
        HTTP("http"),
        /**
         * The PLUGINS value.
         */
        PLUGINS("plugins"),
        /**
         * The INGEST value.
         */
        INGEST("ingest"),
        /**
         * The AGGREGATIONS value.
         */
        AGGREGATIONS("aggregations"),
        /**
         * The INDICES value.
         */
        INDICES("indices"),
        /**
         * The SEARCH_PIPELINES value.
         */
        SEARCH_PIPELINES("search_pipelines");

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

        /**
         * Return all available metrics.
         * See {@link Metric}
         *
         * @return the all metrics
         */
        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }

        /**
         * Return "the default" set of metrics.
         * Similar to {@link #allMetrics()} except {@link Metric#SEARCH_PIPELINES} metric is not included.
         * <br>
         * The motivation to define the default set of metrics was to keep the default response
         * size at bay. Metrics that are NOT included in the default set were typically introduced later
         * and are considered to contain specific type of information that is not usually useful unless you
         * know that you really need it.
         *
         * @return the default metrics
         */
        public static Set<String> defaultMetrics() {
            return allMetrics().stream().filter(metric -> !(metric.equals(SEARCH_PIPELINES.metricName()))).collect(Collectors.toSet());
        }
    }
}
