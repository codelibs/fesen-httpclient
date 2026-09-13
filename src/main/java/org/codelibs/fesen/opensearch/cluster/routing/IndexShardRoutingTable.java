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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.Randomness;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.collect.MapBuilder;
import org.codelibs.fesen.opensearch.common.util.set.Sets;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances of a single shard.
 * Each OpenSearch index consists of multiple shards, each shard encapsulates
 * a disjoint set of the index data and each shard has one or more instances
 * referred to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexShardRoutingTable extends AbstractDiffable<IndexShardRoutingTable> implements Iterable<ShardRouting> {

    final ShardShuffler shuffler;
    // Shuffler for weighted round-robin shard routing. This uses rotation to permute shards.
    final ShardShuffler shufflerForWeightedRouting;
    final ShardId shardId;

    final ShardRouting primary;
    final List<ShardRouting> replicas;
    final List<ShardRouting> shards;
    final List<ShardRouting> activeShards;
    final List<ShardRouting> assignedShards;
    final Set<String> allAllocationIds;
    final boolean allShardsStarted;

    private volatile Map<AttributesKey, AttributesRoutings> activeShardsByAttributes = emptyMap();
    private volatile Map<AttributesKey, AttributesRoutings> initializingShardsByAttributes = emptyMap();
    private final Object shardsByAttributeMutex = new Object();
    private final Object shardsByWeightMutex = new Object();
    private volatile Map<WeightedRoutingKey, WeightedShardRoutings> activeShardsByWeight = emptyMap();
    private volatile Map<WeightedRoutingKey, WeightedShardRoutings> initializingShardsByWeight = emptyMap();

    private static final Logger logger = LogManager.getLogger(IndexShardRoutingTable.class);

    /**
     * The initializing list, including ones that are initializing on a target node because of relocation.
     * If we can come up with a better variable name, it would be nice...
     */
    final List<ShardRouting> allInitializingShards;

    IndexShardRoutingTable(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shufflerForWeightedRouting = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableList(shards);

        ShardRouting primary = null;
        List<ShardRouting> replicas = new ArrayList<>();
        List<ShardRouting> activeShards = new ArrayList<>();
        List<ShardRouting> assignedShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        Set<String> allAllocationIds = new HashSet<>();
        boolean allShardsStarted = true;
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.isSearchOnly()) {
                // mark search only shards as initializing or assigned, but do not add to
                // the allAllocationId set. Cluster Manager will filter out search replica allocationIds in
                // the in-sync set that is sent to primaries, but they are still included in the routing table.
                // This ensures the primaries do not validate these ids exist in tracking nor are included
                // in the unavailableInSyncShards set.
                if (shard.relocating()) {
                    allInitializingShards.add(shard.getTargetRelocatingShard());
                    assignedShards.add(shard.getTargetRelocatingShard());
                }
                if (shard.assignedToNode()) {
                    assignedShards.add(shard);
                }
                assert shard.allocationId() == null || allAllocationIds.contains(shard.allocationId().getId()) == false
                    : "Search replicas should not be part of the allAllocationId set";
                continue;
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                allInitializingShards.add(shard.getTargetRelocatingShard());
                allAllocationIds.add(shard.getTargetRelocatingShard().allocationId().getId());

                assert shard.assignedToNode() : "relocating from unassigned " + shard;
                assert shard.getTargetRelocatingShard().assignedToNode() : "relocating to unassigned " + shard.getTargetRelocatingShard();
                assignedShards.add(shard.getTargetRelocatingShard());
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
                allAllocationIds.add(shard.allocationId().getId());
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.allShardsStarted = allShardsStarted;
        this.primary = primary;
        this.replicas = Collections.unmodifiableList(replicas);
        this.activeShards = Collections.unmodifiableList(activeShards);
        this.assignedShards = Collections.unmodifiableList(assignedShards);
        this.allInitializingShards = Collections.unmodifiableList(allInitializingShards);
        this.allAllocationIds = Collections.unmodifiableSet(allAllocationIds);
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int size() {
        return shards.size();
    }

    /**
     * Returns a {@link List} of shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> shards() {
        return this.shards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.shardId().getIndex().writeTo(out);
        Builder.writeToThin(this, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexShardRoutingTable that = (IndexShardRoutingTable) o;

        if (!shardId.equals(that.shardId)) return false;
        return shards.size() == that.shards.size() && shards.containsAll(that.shards) && that.shards.containsAll(shards);
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    static class AttributesKey {

        final List<String> attributes;

        AttributesKey(List<String> attributes) {
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            return attributes.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AttributesKey && attributes.equals(((AttributesKey) obj).attributes);
        }
    }

    static class AttributesRoutings {

        public final List<ShardRouting> withSameAttribute;
        public final List<ShardRouting> withoutSameAttribute;
        public final int totalSize;

        AttributesRoutings(List<ShardRouting> withSameAttribute, List<ShardRouting> withoutSameAttribute) {
            this.withSameAttribute = withSameAttribute;
            this.withoutSameAttribute = withoutSameAttribute;
            this.totalSize = withoutSameAttribute.size() + withSameAttribute.size();
        }
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    /**
     * Key for WeightedRouting Shard Iterator
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.4.0")
    public static class WeightedRoutingKey {
        private final WeightedRouting weightedRouting;

        public WeightedRoutingKey(WeightedRouting weightedRouting) {
            this.weightedRouting = weightedRouting;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WeightedRoutingKey key = (WeightedRoutingKey) o;
            if (!weightedRouting.equals(key.weightedRouting)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = weightedRouting.hashCode();
            return result;
        }
    }

    /**
     * Holder class for shard routing(s) which are classified and stored based on their weights.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.14.0")
    public static class WeightedShardRoutings {
        private final List<ShardRouting> shardRoutingsWithWeight;
        private final List<ShardRouting> shardRoutingWithoutWeight;

        public WeightedShardRoutings(List<ShardRouting> shardRoutingsWithWeight, List<ShardRouting> shardRoutingWithoutWeight) {
            this.shardRoutingsWithWeight = Collections.unmodifiableList(shardRoutingsWithWeight);
            this.shardRoutingWithoutWeight = Collections.unmodifiableList(shardRoutingWithoutWeight);
        }

        public List<ShardRouting> getShardRoutingsWithWeight() {
            return shardRoutingsWithWeight;
        }

        public List<ShardRouting> getShardRoutingWithoutWeight() {
            return shardRoutingWithoutWeight;
        }
    }

    /**
     * Builder of an index shard routing table.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private ShardId shardId;
        private final List<ShardRouting> shards;

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = new ArrayList<>();
        }

        public Builder addShard(ShardRouting shardEntry) {
            shards.add(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            // don't allow more than one shard copy with same id to be allocated to same node
            assert distinctNodes(shards) : "more than one shard with same id assigned to same node (shards: " + shards + ")";
            return new IndexShardRoutingTable(shardId, Collections.unmodifiableList(new ArrayList<>(shards)));
        }

        static boolean distinctNodes(List<ShardRouting> shards) {
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shard : shards) {
                if (shard.assignedToNode()) {
                    if (nodes.add(shard.currentNodeId()) == false) {
                        return false;
                    }
                    if (shard.relocating()) {
                        if (nodes.add(shard.relocatingNodeId()) == false) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, Index index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ShardRouting shard = new ShardRouting(shardId, in);
                builder.addShard(shard);
            }

            return builder.build();
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());

            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }

        public static void writeVerifiableTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());
            out.writeVInt(indexShard.shards.size());
            // Order allocated shards by allocationId
            AtomicInteger assignedShardCount = new AtomicInteger();
            indexShard.shards.stream()
                .filter(shardRouting -> shardRouting.allocationId() != null)
                .sorted(Comparator.comparing(o -> o.allocationId().getId()))
                .forEach(shardRouting -> {
                    try {
                        assignedShardCount.getAndIncrement();
                        shardRouting.writeToThin(out);
                    } catch (IOException e) {
                        logger.error(() -> new ParameterizedMessage("Failed to write shard {}. Exception {}", indexShard, e));
                        throw new RuntimeException("Failed to write IndexShardRoutingTable", e);
                    }
                });
            // is primary assigned
            out.writeBoolean(indexShard.primaryShard().allocationId() != null);
            out.writeVInt(indexShard.shards.size() - assignedShardCount.get());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexShardRoutingTable(").append(shardId()).append("){");
        final int numShards = shards.size();
        for (int i = 0; i < numShards; i++) {
            sb.append(shards.get(i).shortSummary());
            if (i < numShards - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
