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

import org.codelibs.fesen.opensearch.Build;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.service.ReportingService;
import org.codelibs.fesen.opensearch.http.HttpInfo;
import org.codelibs.fesen.opensearch.ingest.IngestInfo;
import org.codelibs.fesen.opensearch.monitor.jvm.JvmInfo;
import org.codelibs.fesen.opensearch.monitor.os.OsInfo;
import org.codelibs.fesen.opensearch.monitor.process.ProcessInfo;
import org.codelibs.fesen.opensearch.search.aggregations.support.AggregationInfo;
import org.codelibs.fesen.opensearch.search.pipeline.SearchPipelineInfo;
import org.codelibs.fesen.opensearch.threadpool.ThreadPoolInfo;
import org.codelibs.fesen.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 *
 * @opensearch.internal
 */
public class NodeInfo extends BaseNodeResponse {

    private Version version;
    private Build build;

    @Nullable
    private Settings settings;

    /**
     * Do not expose this map to other classes. For type safety, use {@link #getInfo(Class)}
     * to retrieve items from this map and {@link #addInfoIfNonNull(Class, ReportingService.Info)}
     * to retrieve items from it.
     */
    private Map<Class<? extends ReportingService.Info>, ReportingService.Info> infoMap = new HashMap<>();

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    /**
     * Creates a new NodeInfo by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public NodeInfo(StreamInput in) throws IOException {
        super(in);
        version = in.readVersion();
        build = in.readBuild();
        if (in.readBoolean()) {
            totalIndexingBuffer = new ByteSizeValue(in.readLong());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
        }
        addInfoIfNonNull(OsInfo.class, in.readOptionalWriteable(OsInfo::new));
        addInfoIfNonNull(ProcessInfo.class, in.readOptionalWriteable(ProcessInfo::new));
        addInfoIfNonNull(JvmInfo.class, in.readOptionalWriteable(JvmInfo::new));
        addInfoIfNonNull(ThreadPoolInfo.class, in.readOptionalWriteable(ThreadPoolInfo::new));
        addInfoIfNonNull(TransportInfo.class, in.readOptionalWriteable(TransportInfo::new));
        addInfoIfNonNull(HttpInfo.class, in.readOptionalWriteable(HttpInfo::new));
        addInfoIfNonNull(PluginsAndModules.class, in.readOptionalWriteable(PluginsAndModules::new));
        addInfoIfNonNull(IngestInfo.class, in.readOptionalWriteable(IngestInfo::new));
        addInfoIfNonNull(AggregationInfo.class, in.readOptionalWriteable(AggregationInfo::new));
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            addInfoIfNonNull(SearchPipelineInfo.class, in.readOptionalWriteable(SearchPipelineInfo::new));
        }
    }

    /**
     * Creates a new NodeInfo.
     *
     * @param version the version
     * @param build the build
     * @param node the node
     * @param settings the settings
     * @param os the OS
     * @param process the process
     * @param jvm the JVM
     * @param threadPool the thread pool
     * @param transport the transport
     * @param http the HTTP
     * @param plugins the plugins
     * @param ingest the ingest
     * @param aggsInfo the aggs info
     * @param totalIndexingBuffer the total indexing buffer
     * @param searchPipelineInfo the search pipeline info
     */
    public NodeInfo(
        Version version,
        Build build,
        DiscoveryNode node,
        @Nullable Settings settings,
        @Nullable OsInfo os,
        @Nullable ProcessInfo process,
        @Nullable JvmInfo jvm,
        @Nullable ThreadPoolInfo threadPool,
        @Nullable TransportInfo transport,
        @Nullable HttpInfo http,
        @Nullable PluginsAndModules plugins,
        @Nullable IngestInfo ingest,
        @Nullable AggregationInfo aggsInfo,
        @Nullable ByteSizeValue totalIndexingBuffer,
        @Nullable SearchPipelineInfo searchPipelineInfo
    ) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        addInfoIfNonNull(OsInfo.class, os);
        addInfoIfNonNull(ProcessInfo.class, process);
        addInfoIfNonNull(JvmInfo.class, jvm);
        addInfoIfNonNull(ThreadPoolInfo.class, threadPool);
        addInfoIfNonNull(TransportInfo.class, transport);
        addInfoIfNonNull(HttpInfo.class, http);
        addInfoIfNonNull(PluginsAndModules.class, plugins);
        addInfoIfNonNull(IngestInfo.class, ingest);
        addInfoIfNonNull(AggregationInfo.class, aggsInfo);
        addInfoIfNonNull(SearchPipelineInfo.class, searchPipelineInfo);
        this.totalIndexingBuffer = totalIndexingBuffer;
    }

    /**
     * The current OpenSearch version
     *
     * @return the version
     */
    public Version getVersion() {
        return version;
    }

    /**
     * The build version of the node.
     *
     * @return the build
     */
    public Build getBuild() {
        return this.build;
    }

    /**
     * The settings of the node.
     *
     * @return the settings
     */
    @Nullable
    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Get a particular info object, e.g. {@link JvmInfo} or {@link OsInfo}. This
     * generic method handles all casting in order to spare client classes the
     * work of explicit casts. This {@link NodeInfo} class guarantees type
     * safety for these stored info blocks.
     *
     * @param clazz Class for retrieval.
     * @param <T>   Specific subtype of ReportingService.Info to retrieve.
     * @return      An object of type T.
     */
    public <T extends ReportingService.Info> T getInfo(Class<T> clazz) {
        return clazz.cast(infoMap.get(clazz));
    }

    /**
     * Returns the total indexing buffer.
     *
     * @return the total indexing buffer
     */
    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    /**
     * Add a value to the map of information blocks. This method guarantees the
     * type safety of the storage of heterogeneous types of reporting service information.
     */
    private <T extends ReportingService.Info> void addInfoIfNonNull(Class<T> clazz, T info) {
        if (info != null) {
            infoMap.put(clazz, info);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(version.id);
        out.writeBuild(build);
        if (totalIndexingBuffer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(totalIndexingBuffer.getBytes());
        }
        if (settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Settings.writeSettingsToStream(settings, out);
        }
        out.writeOptionalWriteable(getInfo(OsInfo.class));
        out.writeOptionalWriteable(getInfo(ProcessInfo.class));
        out.writeOptionalWriteable(getInfo(JvmInfo.class));
        out.writeOptionalWriteable(getInfo(ThreadPoolInfo.class));
        out.writeOptionalWriteable(getInfo(TransportInfo.class));
        out.writeOptionalWriteable(getInfo(HttpInfo.class));
        out.writeOptionalWriteable(getInfo(PluginsAndModules.class));
        out.writeOptionalWriteable(getInfo(IngestInfo.class));
        out.writeOptionalWriteable(getInfo(AggregationInfo.class));
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeOptionalWriteable(getInfo(SearchPipelineInfo.class));
        }
    }

    /**
     * Returns the builder.
     *
     * @param version the version
     * @param build the build
     * @param node the node
     * @return the builder
     */
    public static NodeInfo.Builder builder(Version version, Build build, DiscoveryNode node) {
        return new Builder(version, build, node);
    }

    /**
     * Builder class to accommodate new Info types being added to NodeInfo.
     */
    public static class Builder {
        private final Version version;
        private final Build build;
        private final DiscoveryNode node;

        private Builder(Version version, Build build, DiscoveryNode node) {
            this.version = version;
            this.build = build;
            this.node = node;
        }

        private Settings settings;
        private OsInfo os;
        private ProcessInfo process;
        private JvmInfo jvm;
        private ThreadPoolInfo threadPool;
        private TransportInfo transport;
        private HttpInfo http;
        private PluginsAndModules plugins;
        private IngestInfo ingest;
        private AggregationInfo aggsInfo;
        private ByteSizeValue totalIndexingBuffer;
        private SearchPipelineInfo searchPipelineInfo;

        /**
         * Builds this instance.
         *
         * @return the new instance
         */
        public NodeInfo build() {
            return new NodeInfo(
                version,
                build,
                node,
                settings,
                os,
                process,
                jvm,
                threadPool,
                transport,
                http,
                plugins,
                ingest,
                aggsInfo,
                totalIndexingBuffer,
                searchPipelineInfo
            );
        }

    }

}
