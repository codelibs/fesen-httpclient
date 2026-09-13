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

package org.codelibs.fesen.opensearch.cluster.node;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.common.Booleans;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.util.set.Sets;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.VerifiableWriteable;
import org.codelibs.fesen.opensearch.core.common.transport.TransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DiscoveryNodes extends AbstractDiffable<DiscoveryNodes> implements Iterable<DiscoveryNode>, VerifiableWriteable {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();

    private final Map<String, DiscoveryNode> nodes;
    private final Map<String, DiscoveryNode> dataNodes;
    private final Map<String, DiscoveryNode> warmNodes;
    private final Map<String, DiscoveryNode> clusterManagerNodes;
    private final Map<String, DiscoveryNode> ingestNodes;

    private final String clusterManagerNodeId;
    private final String localNodeId;
    private final Version minNonClientNodeVersion;
    private final Version maxNonClientNodeVersion;
    private final Version maxNodeVersion;
    private final Version minNodeVersion;

    private DiscoveryNodes(
        final Map<String, DiscoveryNode> nodes,
        final Map<String, DiscoveryNode> dataNodes,
        final Map<String, DiscoveryNode> warmNodes,
        final Map<String, DiscoveryNode> clusterManagerNodes,
        final Map<String, DiscoveryNode> ingestNodes,
        String clusterManagerNodeId,
        String localNodeId,
        Version minNonClientNodeVersion,
        Version maxNonClientNodeVersion,
        Version maxNodeVersion,
        Version minNodeVersion
    ) {
        this.nodes = Collections.unmodifiableMap(nodes);
        this.dataNodes = Collections.unmodifiableMap(dataNodes);
        this.warmNodes = Collections.unmodifiableMap(warmNodes);
        this.clusterManagerNodes = Collections.unmodifiableMap(clusterManagerNodes);
        this.ingestNodes = Collections.unmodifiableMap(ingestNodes);
        this.clusterManagerNodeId = clusterManagerNodeId;
        this.localNodeId = localNodeId;
        this.minNonClientNodeVersion = minNonClientNodeVersion;
        this.maxNonClientNodeVersion = maxNonClientNodeVersion;
        this.minNodeVersion = minNodeVersion;
        this.maxNodeVersion = maxNodeVersion;
    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.values().iterator();
    }

    /**
     * Get the number of known nodes
     *
     * @return number of nodes
     */
    public int getSize() {
        return nodes.size();
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getNodes() {
        return this.nodes;
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getDataNodes() {
        return this.dataNodes;
    }

    /**
     * Get a node by its id
     *
     * @param nodeId id of the wanted node
     * @return wanted node if it exists. Otherwise <code>null</code>
     */
    public DiscoveryNode get(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Get the id of the cluster-manager node
     *
     * @return id of the cluster-manager
     */
    public String getClusterManagerNodeId() {
        return this.clusterManagerNodeId;
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String getLocalNodeId() {
        return this.localNodeId;
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode getLocalNode() {
        return nodes.get(localNodeId);
    }

    /**
     * Returns the cluster-manager node, or {@code null} if there is no cluster-manager node
     */
    @Nullable
    public DiscoveryNode getClusterManagerNode() {
        if (clusterManagerNodeId != null) {
            return nodes.get(clusterManagerNodeId);
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes: \n");
        for (DiscoveryNode node : this) {
            sb.append("   ").append(node);
            if (node == getLocalNode()) {
                sb.append(", local");
            }
            if (node == getClusterManagerNode()) {
                sb.append(", cluster-manager");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeToUtil((output, value) -> value.writeTo(output), out);
    }

    public void writeToWithAttribute(StreamOutput out) throws IOException {
        writeToUtil((output, value) -> value.writeToWithAttribute(output), out);
    }

    public void writeToUtil(final Writer<DiscoveryNode> writer, StreamOutput out) throws IOException {
        writeClusterManager(out);
        out.writeVInt(nodes.size());
        for (DiscoveryNode node : this) {
            writer.write(out, node);
        }
    }

    @Override
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        writeClusterManager(out);
        out.writeMapValues(nodes, (stream, val) -> val.writeVerifiableTo((BufferedChecksumStreamOutput) stream));
    }

    private void writeClusterManager(StreamOutput out) throws IOException {
        if (clusterManagerNodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(clusterManagerNodeId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscoveryNodes that = (DiscoveryNodes) o;
        return Objects.equals(nodes, that.nodes)
            && Objects.equals(dataNodes, that.dataNodes)
            && Objects.equals(clusterManagerNodes, that.clusterManagerNodes)
            && Objects.equals(ingestNodes, that.ingestNodes)
            && Objects.equals(clusterManagerNodeId, that.clusterManagerNodeId)
            && Objects.equals(localNodeId, that.localNodeId)
            && Objects.equals(minNonClientNodeVersion, that.minNonClientNodeVersion)
            && Objects.equals(maxNonClientNodeVersion, that.maxNonClientNodeVersion)
            && Objects.equals(maxNodeVersion, that.maxNodeVersion)
            && Objects.equals(minNodeVersion, that.minNodeVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nodes,
            dataNodes,
            clusterManagerNodes,
            ingestNodes,
            clusterManagerNodeId,
            localNodeId,
            minNonClientNodeVersion,
            maxNonClientNodeVersion,
            maxNodeVersion,
            minNodeVersion
        );
    }

    public static DiscoveryNodes readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        Builder builder = new Builder();
        if (in.readBoolean()) {
            builder.clusterManagerNodeId(in.readString());
        }
        if (localNode != null) {
            builder.localNodeId(localNode.getId());
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DiscoveryNode node = new DiscoveryNode(in);
            if (localNode != null && node.getId().equals(localNode.getId())) {
                // reuse the same instance of our address and local node id for faster equality
                node = localNode;
            }
            // some one already built this and validated it's OK, skip the n2 scans
            assert builder.validateAdd(node) == null : "building disco nodes from network doesn't pass preflight: "
                + builder.validateAdd(node);
            builder.putUnsafe(node);
        }
        return builder.build();
    }

    public static Diff<DiscoveryNodes> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return AbstractDiffable.readDiffFrom(in1 -> readFrom(in1, localNode), in);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of a map of discovery nodes.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Map<String, DiscoveryNode> nodes;
        private String clusterManagerNodeId;
        private String localNodeId;

        public Builder() {
            nodes = new HashMap<>();
        }

        private void putUnsafe(DiscoveryNode node) {
            nodes.put(node.getId(), node);
        }

        public Builder clusterManagerNodeId(String clusterManagerNodeId) {
            this.clusterManagerNodeId = clusterManagerNodeId;
            return this;
        }

        public Builder localNodeId(String localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        /**
         * Checks that a node can be safely added to this node collection.
         *
         * @return null if all is OK or an error message explaining why a node can not be added.
         * <p>
         * Note: if this method returns a non-null value, calling {@link #add(DiscoveryNode)} will fail with an
         * exception
         */
        private String validateAdd(DiscoveryNode node) {
            for (final DiscoveryNode existingNode : nodes.values()) {
                if (node.getAddress().equals(existingNode.getAddress()) && node.getId().equals(existingNode.getId()) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode + " with same address";
                }
                if (node.getId().equals(existingNode.getId()) && node.equals(existingNode) == false) {
                    return "can't add node "
                        + node
                        + ", found existing node "
                        + existingNode
                        + " with the same id but is a different node instance";
                }
            }
            return null;
        }

        public DiscoveryNodes build() {
            final Map<String, DiscoveryNode> dataNodesBuilder = new HashMap<>();
            final Map<String, DiscoveryNode> warmNodesBuilder = new HashMap<>();
            final Map<String, DiscoveryNode> clusterManagerNodesBuilder = new HashMap<>();
            final Map<String, DiscoveryNode> ingestNodesBuilder = new HashMap<>();
            Version minNodeVersion = null;
            Version maxNodeVersion = null;
            Version minNonClientNodeVersion = null;
            Version maxNonClientNodeVersion = null;
            for (final Map.Entry<String, DiscoveryNode> nodeEntry : nodes.entrySet()) {
                if (nodeEntry.getValue().isDataNode()) {
                    dataNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                if (nodeEntry.getValue().isWarmNode()) {
                    warmNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                if (nodeEntry.getValue().isClusterManagerNode()) {
                    clusterManagerNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                final Version version = nodeEntry.getValue().getVersion();
                if (nodeEntry.getValue().isDataNode() || nodeEntry.getValue().isClusterManagerNode()) {
                    if (minNonClientNodeVersion == null) {
                        minNonClientNodeVersion = version;
                        maxNonClientNodeVersion = version;
                    } else {
                        minNonClientNodeVersion = Version.min(minNonClientNodeVersion, version);
                        maxNonClientNodeVersion = Version.max(maxNonClientNodeVersion, version);
                    }
                }
                if (nodeEntry.getValue().isIngestNode()) {
                    ingestNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                minNodeVersion = minNodeVersion == null ? version : Version.min(minNodeVersion, version);
                maxNodeVersion = maxNodeVersion == null ? version : Version.max(maxNodeVersion, version);
            }

            return new DiscoveryNodes(
                nodes,
                dataNodesBuilder,
                warmNodesBuilder,
                clusterManagerNodesBuilder,
                ingestNodesBuilder,
                clusterManagerNodeId,
                localNodeId,
                minNonClientNodeVersion == null ? Version.CURRENT : minNonClientNodeVersion,
                maxNonClientNodeVersion == null ? Version.CURRENT : maxNonClientNodeVersion,
                maxNodeVersion == null ? Version.CURRENT : maxNodeVersion,
                minNodeVersion == null ? Version.CURRENT : minNodeVersion
            );
        }
    }
}
