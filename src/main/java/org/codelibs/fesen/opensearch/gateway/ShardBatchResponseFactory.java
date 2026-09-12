/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.gateway;

import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.codelibs.fesen.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.codelibs.fesen.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata;
import org.codelibs.fesen.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;

import java.util.Map;

/**
 * A factory class to create new responses of batch transport actions like
 * {@link TransportNodesListGatewayStartedShardsBatch} or {@link org.codelibs.fesen.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch}
 *
 * @param <T> Node level response returned by batch transport actions.
 * @param <V> Shard level metadata returned by batch transport actions.
 */
public class ShardBatchResponseFactory<T extends BaseNodeResponse, V> {
    private final boolean primary;

    public ShardBatchResponseFactory(boolean primary) {
        this.primary = primary;
    }

    public T getNewResponse(DiscoveryNode node, Map<ShardId, V> shardData) {
        if (primary) {
            return (T) new NodeGatewayStartedShardsBatch(node, (Map<ShardId, GatewayStartedShard>) shardData);
        } else {
            return (T) new NodeStoreFilesMetadataBatch(node, (Map<ShardId, NodeStoreFilesMetadata>) shardData);
        }
    }

    public Map<ShardId, V> getShardBatchData(T response) {
        if (primary) {
            return (Map<ShardId, V>) ((NodeGatewayStartedShardsBatch) response).getNodeGatewayStartedShardsBatch();
        } else {
            return (Map<ShardId, V>) ((NodeStoreFilesMetadataBatch) response).getNodeStoreFilesMetadataBatch();
        }
    }

}
