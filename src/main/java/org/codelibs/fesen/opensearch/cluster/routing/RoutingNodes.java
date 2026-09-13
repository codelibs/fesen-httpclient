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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.Randomness;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.core.Assertions;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * {@link RoutingNodes} represents a copy the routing information contained in the {@link ClusterState cluster state}.
 * It can be either initialized as mutable or immutable (see {@link #RoutingNodes(ClusterState, boolean)}), allowing
 * or disallowing changes to its elements.
 * <p>
 * The main methods used to update routing entries are:
 * <ul>
 * <li> {@link #initializeShard} initializes an unassigned shard.
 * <li> {@link #startShard} starts an initializing shard / completes relocation of a shard.
 * <li> {@link #relocateShard} starts relocation of a started shard.
 * <li> {@link #failShard} fails/cancels an assigned shard.
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RoutingNodes implements Iterable<RoutingNode> {

    private final Metadata metadata;

    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    private final Map<ShardId, List<ShardRouting>> assignedShards = new HashMap<>();

    private final boolean readOnly;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private final Map<String, Set<String>> nodesPerAttributeNames;
    private final Map<String, Set<String>> searchNodesPerAttributeNames;
    private final Map<String, Recoveries> recoveriesPerNode = new HashMap<>();
    private final Map<String, Recoveries> initialReplicaRecoveries = new HashMap<>();
    private final Map<String, Recoveries> initialPrimaryRecoveries = new HashMap<>();

    public RoutingNodes(ClusterState clusterState) {
        this(clusterState, true);
    }

    public RoutingNodes(ClusterState clusterState, boolean readOnly) {
        this.metadata = clusterState.getMetadata();
        this.readOnly = readOnly;
        final RoutingTable routingTable = clusterState.routingTable();
        this.nodesPerAttributeNames = Collections.synchronizedMap(new HashMap<>());
        this.searchNodesPerAttributeNames = Collections.synchronizedMap(new HashMap<>());

        // fill in the nodeToShards with the "live" nodes
        for (final DiscoveryNode cursor : clusterState.nodes().getDataNodes().values()) {
            String nodeId = cursor.getId();
            this.nodesToShards.put(cursor.getId(), new RoutingNode(nodeId, clusterState.nodes().get(nodeId)));
        }

        // fill in the inverse of node -> shards allocated
        // also fill replicaSet information
        for (final IndexRoutingTable indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable) {
                IndexMetadata idxMetadata = metadata.index(indexShard.shardId().getIndex());
                boolean isSearchOnlyClusterBlockEnabled = false;
                if (idxMetadata != null) {
                    isSearchOnlyClusterBlockEnabled = idxMetadata.getSettings()
                        .getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false);
                }
                if (isSearchOnlyClusterBlockEnabled == false) {
                    assert indexShard.primary != null : "Primary shard routing can't be null for non-search-only indices";
                }
                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    if (shard.assignedToNode()) {
                        RoutingNode routingNode = this.nodesToShards.computeIfAbsent(
                            shard.currentNodeId(),
                            k -> new RoutingNode(shard.currentNodeId(), clusterState.nodes().get(shard.currentNodeId()))
                        );
                        routingNode.add(shard);
                        assignedShardsAdd(shard);
                        if (shard.relocating()) {
                            relocatingShards++;
                            // Add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            routingNode = nodesToShards.computeIfAbsent(
                                shard.relocatingNodeId(),
                                k -> new RoutingNode(shard.relocatingNodeId(), clusterState.nodes().get(shard.relocatingNodeId()))
                            );
                            ShardRouting targetShardRouting = shard.getTargetRelocatingShard();
                            addInitialRecovery(targetShardRouting, indexShard.primary);
                            routingNode.add(targetShardRouting);
                            assignedShardsAdd(targetShardRouting);
                        } else if (shard.initializing()) {
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                            addInitialRecovery(shard, indexShard.primary);
                        }
                    } else {
                        unassignedShards.add(shard);
                    }
                }
            }
        }
        assert nodesToShards.values().stream().allMatch(RoutingNode::invariant);
    }

    private void addInitialRecovery(ShardRouting routing, ShardRouting initialPrimaryShard) {
        updateRecoveryCounts(routing, true, initialPrimaryShard);
    }

    private void updateRecoveryCounts(final ShardRouting routing, final boolean increment, @Nullable final ShardRouting primary) {

        final int howMany = increment ? 1 : -1;
        assert routing.initializing() : "routing must be initializing: " + routing;

        IndexMetadata idxMetadata = metadata.index(routing.index());
        boolean isSearchOnlyClusterBlockEnabled = false;
        if (idxMetadata != null) {
            isSearchOnlyClusterBlockEnabled = idxMetadata.getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false);
        }

        // TODO: check primary == null || primary.active() after all tests properly add ReplicaAfterPrimaryActiveAllocationDecider
        if (isSearchOnlyClusterBlockEnabled == false) {
            assert primary == null || primary.assignedToNode() : "shard is initializing but its primary is not assigned to a node";
        }

        // Primary shard routing, excluding the relocating primaries.
        if (routing.primary() && (primary == null || primary == routing)) {
            assert routing.relocatingNodeId() == null : "Routing must be a non relocating primary";
            Recoveries.getOrAdd(initialPrimaryRecoveries, routing.currentNodeId()).addIncoming(howMany);
            return;
        }

        Recoveries.getOrAdd(getRecoveries(routing), routing.currentNodeId()).addIncoming(howMany);

        if (routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            // add/remove corresponding outgoing recovery on node with primary shard
            if (primary == null) {
                throw new IllegalStateException("shard is peer recovering but primary is unassigned");
            }

            Recoveries.getOrAdd(getRecoveries(routing), primary.currentNodeId()).addOutgoing(howMany);

            if (increment == false && routing.primary() && routing.relocatingNodeId() != null) {
                // primary is done relocating, move non-primary recoveries from old primary to new primary
                for (ShardRouting assigned : assignedShards(routing.shardId())) {
                    if (assigned.primary() == false
                        && assigned.initializing()
                        && assigned.recoverySource().getType() == RecoverySource.Type.PEER) {
                        Map<String, Recoveries> recoveriesToUpdate = getRecoveries(assigned);
                        Recoveries.getOrAdd(recoveriesToUpdate, routing.relocatingNodeId()).addOutgoing(-1);
                        Recoveries.getOrAdd(recoveriesToUpdate, routing.currentNodeId()).addOutgoing(1);
                    }
                }

            }
        }
    }

    private Map<String, Recoveries> getRecoveries(ShardRouting routing) {
        if (routing.unassignedReasonIndexCreated() && !routing.primary()) {
            return initialReplicaRecoveries;
        } else {
            return recoveriesPerNode;
        }
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return Collections.unmodifiableCollection(nodesToShards.values()).iterator();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public Stream<RoutingNode> stream() {
        return nodesToShards.values().stream();
    }

    /**
     * Returns all shards that are not in the state UNASSIGNED with the same shard
     * ID as the given shard.
     */
    public List<ShardRouting> assignedShards(ShardId shardId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        return replicaSet == null ? EMPTY : Collections.unmodifiableList(replicaSet);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (ShardRouting shardEntry : unassignedShards) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    private static final List<ShardRouting> EMPTY = Collections.emptyList();

    private void assignedShardsAdd(ShardRouting shard) {
        assert shard.unassigned() == false : "unassigned shard " + shard + " cannot be added to list of assigned shards";
        List<ShardRouting> shards = assignedShards.computeIfAbsent(shard.shardId(), k -> new ArrayList<>());
        assert assertInstanceNotInList(shard, shards) : "shard " + shard + " cannot appear twice in list of assigned shards";
        shards.add(shard);
    }

    private boolean assertInstanceNotInList(ShardRouting shard, List<ShardRouting> shards) {
        for (ShardRouting s : shards) {
            assert s != shard;
        }
        return true;
    }

    /**
     * Unassigned shard list.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class UnassignedShards implements Iterable<ShardRouting> {

        private final RoutingNodes nodes;
        private final List<ShardRouting> unassigned;
        private final List<ShardRouting> ignored;

        private int primaries = 0;
        private int ignoredPrimaries = 0;

        public UnassignedShards(RoutingNodes nodes) {
            this.nodes = nodes;
            unassigned = new ArrayList<>();
            ignored = new ArrayList<>();
        }

        public void add(ShardRouting shardRouting) {
            if (shardRouting.primary()) {
                primaries++;
            }
            unassigned.add(shardRouting);
        }

        @Override
        public UnassignedIterator iterator() {
            return new UnassignedIterator();
        }

        /**
         * An unassigned iterator.
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        public class UnassignedIterator implements Iterator<ShardRouting> {

            private final ListIterator<ShardRouting> iterator;
            private ShardRouting current;

            public UnassignedIterator() {
                this.iterator = unassigned.listIterator();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ShardRouting next() {
                return current = iterator.next();
            }

            /**
             * Unsupported operation, just there for the interface. Use
             * {@link #removeAndIgnore(AllocationStatus, RoutingChangesObserver)} or
             * {@link #initialize(String, String, long, RoutingChangesObserver)}.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException(
                    "remove is not supported in unassigned iterator," + " use removeAndIgnore or initialize"
                );
            }
        }
    }

    private static boolean isNonRelocatingPrimary(ShardRouting routing) {
        return routing.primary() && routing.relocatingNodeId() == null;
    }

    /**
     * A collection of recoveries.
     *
     * @opensearch.internal
     */
    private static final class Recoveries {
        private static final Recoveries EMPTY = new Recoveries();
        private int incoming = 0;
        private int outgoing = 0;

        void addOutgoing(int howMany) {
            assert outgoing + howMany >= 0 : outgoing + howMany + " must be >= 0";
            outgoing += howMany;
        }

        void addIncoming(int howMany) {
            assert incoming + howMany >= 0 : incoming + howMany + " must be >= 0";
            incoming += howMany;
        }

        public static Recoveries getOrAdd(Map<String, Recoveries> map, String key) {
            Recoveries recoveries = map.get(key);
            if (recoveries == null) {
                recoveries = new Recoveries();
                map.put(key, recoveries);
            }
            return recoveries;
        }
    }
}
