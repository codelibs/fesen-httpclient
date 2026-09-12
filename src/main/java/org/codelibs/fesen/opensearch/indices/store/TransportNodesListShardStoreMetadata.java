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

package org.codelibs.fesen.opensearch.indices.store;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.FailedNodeException;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesResponse;
import org.codelibs.fesen.opensearch.action.support.nodes.TransportNodesAction;
import org.codelibs.fesen.opensearch.cluster.ClusterName;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.env.NodeEnvironment;
import org.codelibs.fesen.opensearch.gateway.AsyncShardFetch;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportRequest;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper.listShardMetadataInternal;

/**
 * Metadata for shard stores from a list of transport nodes
 *
 * @opensearch.internal
 */
public class TransportNodesListShardStoreMetadata extends TransportNodesAction<
    TransportNodesListShardStoreMetadata.Request,
    TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeRequest,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>
    implements
        AsyncShardFetch.Lister<
            TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
            TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store";
    public static final ActionType<NodesStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME, NodesStoreFilesMetadata::new);

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    public TransportNodesListShardStoreMetadata(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        NodeEnvironment nodeEnv,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STORE,
            NodeStoreFilesMetadata.class
        );
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    public void list(
        Map<ShardId, ShardAttributes> shardAttributes,
        DiscoveryNode[] nodes,
        ActionListener<NodesStoreFilesMetadata> listener
    ) {
        assert shardAttributes.size() == 1 : "only one shard should be specified";
        final ShardId shardId = shardAttributes.keySet().iterator().next();
        final String customDataPath = shardAttributes.get(shardId).getCustomDataPath();
        execute(new Request(shardId, customDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadata newNodeResponse(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadata(in);
    }

    @Override
    protected NodesStoreFilesMetadata newResponse(
        Request request,
        List<NodeStoreFilesMetadata> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadata nodeOperation(NodeRequest request) {
        try {
            return new NodeStoreFilesMetadata(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new OpenSearchException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    private StoreFilesMetadata listStoreMetadata(NodeRequest request) throws IOException {
        final ShardId shardId = request.getShardId();
        return listShardMetadataInternal(logger, shardId, nodeEnv, indicesService, request.getCustomDataPath(), settings, clusterService);
    }

    /**
     * The request
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(customDataPath);
        }
    }

    /**
     * Metadata for the nodes store files
     *
     * @opensearch.internal
     */
    public static class NodesStoreFilesMetadata extends BaseNodesResponse<NodeStoreFilesMetadata> {

        public NodesStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
        }

        public NodesStoreFilesMetadata(ClusterName clusterName, List<NodeStoreFilesMetadata> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadata> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeStoreFilesMetadata::readListShardStoreNodeOperationResponse);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadata> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The node request
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            assert customDataPath != null;
            out.writeString(customDataPath);
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    /**
     * The metadata for the node store files
     *
     * @opensearch.internal
     */
    public static class NodeStoreFilesMetadata extends BaseNodeResponse {

        private StoreFilesMetadata storeFilesMetadata;

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
            storeFilesMetadata = new StoreFilesMetadata(in);
        }

        public NodeStoreFilesMetadata(DiscoveryNode node, StoreFilesMetadata storeFilesMetadata) {
            super(node);
            this.storeFilesMetadata = storeFilesMetadata;
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        public static NodeStoreFilesMetadata readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            return new NodeStoreFilesMetadata(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            storeFilesMetadata.writeTo(out);
        }

        @Override
        public String toString() {
            return "[[" + getNode() + "][" + storeFilesMetadata + "]]";
        }
    }
}
