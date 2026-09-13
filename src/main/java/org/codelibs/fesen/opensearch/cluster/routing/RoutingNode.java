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

package org.codelibs.fesen.opensearch.cluster.routing;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A {@link RoutingNode} represents a cluster node associated with a single {@link DiscoveryNode} including all shards
 * that are hosted on that nodes. Each {@link RoutingNode} has a unique node id that can be used to identify the node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RoutingNode implements Iterable<ShardRouting> {

    static class BucketedShards implements Iterable<ShardRouting> {
        private final Tuple<LinkedHashMap<ShardId, ShardRouting>, LinkedHashMap<ShardId, ShardRouting>> shardTuple; // LinkedHashMap to
                                                                                                                    // preserve order

        BucketedShards(LinkedHashMap<ShardId, ShardRouting> primaryShards, LinkedHashMap<ShardId, ShardRouting> replicaShards) {
            this.shardTuple = new Tuple(primaryShards, replicaShards);
        }

        public int size() {
            return this.shardTuple.v1().size() + this.shardTuple.v2().size();
        }

        public ShardRouting get(ShardId shardId) {
            if (this.shardTuple.v1().containsKey(shardId)) {
                return this.shardTuple.v1().get(shardId);
            }
            return this.shardTuple.v2().get(shardId);
        }

        public ShardRouting put(ShardRouting shardRouting) {
            return put(shardRouting.shardId(), shardRouting);
        }

        public ShardRouting put(ShardId shardId, ShardRouting shardRouting) {
            ShardRouting ret;
            if (shardRouting.primary()) {
                ret = this.shardTuple.v1().put(shardId, shardRouting);
                if (this.shardTuple.v2().containsKey(shardId)) {
                    ret = this.shardTuple.v2().remove(shardId);
                }
            } else {
                ret = this.shardTuple.v2().put(shardId, shardRouting);
                if (this.shardTuple.v1().containsKey(shardId)) {
                    ret = this.shardTuple.v1().remove(shardId);
                }
            }

            return ret;
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return Stream.concat(
                Collections.unmodifiableCollection(this.shardTuple.v1().values()).stream(),
                Collections.unmodifiableCollection(this.shardTuple.v2().values()).stream()
            ).iterator();
        }
    }

    static class RelocatingShardsBucket {
        private final LinkedHashSet<ShardRouting> relocatingShards;
        private final LinkedHashSet<ShardRouting> relocatingPrimaryShards;

        RelocatingShardsBucket() {
            relocatingShards = new LinkedHashSet<>();
            relocatingPrimaryShards = new LinkedHashSet<>();
        }

        public boolean add(ShardRouting shard) {
            boolean res = relocatingShards.add(shard);
            if (shard.primary()) {
                relocatingPrimaryShards.add(shard);
            }
            return res;
        }

        public int size() {
            return relocatingShards.size();
        }

        public Set<ShardRouting> getRelocatingShards() {
            return Collections.unmodifiableSet(relocatingShards);
        }

        public Set<ShardRouting> getRelocatingPrimaryShards() {
            return Collections.unmodifiableSet(relocatingPrimaryShards);
        }

        // For assertions/verification
        public boolean invariant() {
            assert relocatingShards.containsAll(relocatingPrimaryShards);
            assert relocatingPrimaryShards.stream().allMatch(ShardRouting::primary);
            assert relocatingPrimaryShards.size() == relocatingShards.stream().filter(ShardRouting::primary).count();
            return true;
        }
    }

    private final String nodeId;

    private final DiscoveryNode node;

    private final BucketedShards shards;

    private final RelocatingShardsBucket relocatingShardsBucket;

    private final LinkedHashSet<ShardRouting> initializingShards;

    private final HashMap<Index, LinkedHashSet<ShardRouting>> shardsByIndex;

    public RoutingNode(String nodeId, DiscoveryNode node, ShardRouting... shardRoutings) {
        this.nodeId = nodeId;
        this.node = node;
        final LinkedHashMap<ShardId, ShardRouting> primaryShards = new LinkedHashMap<>();
        final LinkedHashMap<ShardId, ShardRouting> replicaShards = new LinkedHashMap<>();
        this.shards = new BucketedShards(primaryShards, replicaShards);
        this.relocatingShardsBucket = new RelocatingShardsBucket();
        this.initializingShards = new LinkedHashSet<>();
        this.shardsByIndex = new LinkedHashMap<>();

        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting.initializing()) {
                initializingShards.add(shardRouting);
            } else if (shardRouting.relocating()) {
                relocatingShardsBucket.add(shardRouting);
            }
            shardsByIndex.computeIfAbsent(shardRouting.index(), k -> new LinkedHashSet<>()).add(shardRouting);

            ShardRouting previousValue;
            if (shardRouting.primary()) {
                previousValue = primaryShards.put(shardRouting.shardId(), shardRouting);
            } else {
                previousValue = replicaShards.put(shardRouting.shardId(), shardRouting);
            }

            if (previousValue != null) {
                throw new IllegalArgumentException(
                    "Cannot have two different shards with same shard id " + shardRouting.shardId() + " on same node "
                );
            }
        }

        assert invariant();
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the nodes {@link DiscoveryNode}.
     *
     * @return discoveryNode of this node
     */
    public DiscoveryNode node() {
        return this.node;
    }

    /**
     * Get the id of this node
     * @return id of the node
     */
    public String nodeId() {
        return this.nodeId;
    }

    public int size() {
        return shards.size();
    }

    public Collection<ShardRouting> getInitializingShards() {
        return initializingShards;
    }

    /**
     * Add a new shard to this node
     * @param shard Shard to create on this Node
     */
    void add(ShardRouting shard) {
        if (shards.put(shard) != null) {
            throw new IllegalStateException(
                "Trying to add a shard "
                    + shard.shardId()
                    + " to a node ["
                    + nodeId
                    + "] where it already exists. current ["
                    + shards.get(shard.shardId())
                    + "]. new ["
                    + shard
                    + "]"
            );
        }

        if (shard.initializing()) {
            initializingShards.add(shard);
        } else if (shard.relocating()) {
            relocatingShardsBucket.add(shard);
        }
        shardsByIndex.computeIfAbsent(shard.index(), k -> new LinkedHashSet<>()).add(shard);
    }

    /**
     * Determine the number of shards with a specific state
     * @param states set of states which should be counted
     * @return number of shards
     */
    public int numberOfShardsWithState(ShardRoutingState... states) {
        if (states.length == 1) {
            if (states[0] == ShardRoutingState.INITIALIZING) {
                return initializingShards.size();
            } else if (states[0] == ShardRoutingState.RELOCATING) {
                return relocatingShardsBucket.size();
            }
        }

        int count = 0;
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * The number of shards on this node that will not be eventually relocated.
     */
    public int numberOfOwningShards() {
        return shards.size() - relocatingShardsBucket.size();
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----node_id[").append(nodeId).append("][").append(node == null ? "X" : "V").append("]\n");
        for (ShardRouting entry : shards) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("routingNode ([");
        sb.append(node.getName());
        sb.append("][");
        sb.append(node.getId());
        sb.append("][");
        sb.append(node.getHostName());
        sb.append("][");
        sb.append(node.getHostAddress());
        sb.append("], [");
        sb.append(shards.size());
        sb.append(" assigned shards])");
        return sb.toString();
    }

    boolean invariant() {

        // initializingShards must consistent with that in shards
        Collection<ShardRouting> shardRoutingsInitializing = StreamSupport.stream(shards.spliterator(), false)
            .filter(ShardRouting::initializing)
            .collect(Collectors.toList());
        assert initializingShards.size() == shardRoutingsInitializing.size();
        assert initializingShards.containsAll(shardRoutingsInitializing);

        // relocatingShards must consistent with that in shards
        Collection<ShardRouting> shardRoutingsRelocating = StreamSupport.stream(shards.spliterator(), false)
            .filter(ShardRouting::relocating)
            .collect(Collectors.toList());
        assert relocatingShardsBucket.getRelocatingShards().size() == shardRoutingsRelocating.size();
        assert relocatingShardsBucket.getRelocatingShards().containsAll(shardRoutingsRelocating);

        // relocatingPrimaryShards must be consistent with primary shards that are relocating
        Collection<ShardRouting> primaryShardRoutingsRelocating = StreamSupport.stream(shards.spliterator(), false)
            .filter(ShardRouting::relocating)
            .filter(ShardRouting::primary)
            .collect(Collectors.toList());
        assert relocatingShardsBucket.getRelocatingPrimaryShards().size() == primaryShardRoutingsRelocating.size();
        assert relocatingShardsBucket.getRelocatingPrimaryShards().containsAll(primaryShardRoutingsRelocating);

        // relocatingPrimaryShards and relocatingShards should be consistent
        assert relocatingShardsBucket.invariant();

        final Map<Index, Set<ShardRouting>> shardRoutingsByIndex = StreamSupport.stream(shards.spliterator(), false)
            .collect(Collectors.groupingBy(ShardRouting::index, Collectors.toSet()));
        assert shardRoutingsByIndex.equals(shardsByIndex);

        return true;
    }
}
