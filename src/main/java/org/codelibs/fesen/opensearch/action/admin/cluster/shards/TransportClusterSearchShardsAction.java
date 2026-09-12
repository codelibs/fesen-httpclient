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

package org.codelibs.fesen.opensearch.action.admin.cluster.shards;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.GroupShardsIterator;
import org.codelibs.fesen.opensearch.cluster.routing.ShardIterator;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.indices.IndicesService;
import org.codelibs.fesen.opensearch.search.internal.AliasFilter;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Transport action for searching shards
 *
 * @opensearch.internal
 */
public class TransportClusterSearchShardsAction extends TransportClusterManagerNodeReadAction<
    ClusterSearchShardsRequest,
    ClusterSearchShardsResponse> implements TransportIndicesResolvingAction<ClusterSearchShardsRequest> {

    private final IndicesService indicesService;

    public TransportClusterSearchShardsAction(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterSearchShardsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterSearchShardsRequest::new,
            indexNameExpressionResolver,
            true
        );
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        // all in memory work here...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterSearchShardsRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, resolveIndices(state, request).namesOfConcreteIndicesAsArray());
    }

    @Override
    protected ClusterSearchShardsResponse read(StreamInput in) throws IOException {
        return new ClusterSearchShardsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterSearchShardsRequest request,
        final ClusterState state,
        final ActionListener<ClusterSearchShardsResponse> listener
    ) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = resolveIndices(clusterState, request).namesOfConcreteIndicesAsArray();
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(state, request.routing(), request.indices());
        Map<String, AliasFilter> indicesAndFilters = new HashMap<>();
        Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (String index : concreteIndices) {
            final AliasFilter aliasFilter = indicesService.buildAliasFilter(clusterState, index, indicesAndAliases);
            final String[] aliases = indexNameExpressionResolver.indexAliases(
                clusterState,
                index,
                aliasMetadata -> true,
                true,
                indicesAndAliases
            );
            indicesAndFilters.put(index, new AliasFilter(aliasFilter.getQueryBuilder(), aliases));
        }

        Set<String> nodeIds = new HashSet<>();
        GroupShardsIterator<ShardIterator> groupShardsIterator = clusterService.operationRouting()
            .searchShards(clusterState, concreteIndices, routingMap, request.preference(), null, null, request.slice());
        ShardRouting shard;
        ClusterSearchShardsGroup[] groupResponses = new ClusterSearchShardsGroup[groupShardsIterator.size()];
        int currentGroup = 0;
        for (ShardIterator shardIt : groupShardsIterator) {
            ShardId shardId = shardIt.shardId();
            ShardRouting[] shardRoutings = new ShardRouting[shardIt.size()];
            int currentShard = 0;
            shardIt.reset();
            while ((shard = shardIt.nextOrNull()) != null) {
                shardRoutings[currentShard++] = shard;
                nodeIds.add(shard.currentNodeId());
            }
            groupResponses[currentGroup++] = new ClusterSearchShardsGroup(shardId, shardRoutings);
        }
        DiscoveryNode[] nodes = new DiscoveryNode[nodeIds.size()];
        int currentNode = 0;
        for (String nodeId : nodeIds) {
            nodes[currentNode++] = clusterState.getNodes().get(nodeId);
        }
        listener.onResponse(new ClusterSearchShardsResponse(groupResponses, nodes, indicesAndFilters));
    }

    @Override
    public ResolvedIndices resolveIndices(ClusterSearchShardsRequest request) {
        return ResolvedIndices.of(resolveIndices(clusterService.state(), request));
    }

    private ResolvedIndices.Local.Concrete resolveIndices(ClusterState clusterState, ClusterSearchShardsRequest request) {
        return this.indexNameExpressionResolver.concreteResolvedIndices(clusterState, request);
    }
}
