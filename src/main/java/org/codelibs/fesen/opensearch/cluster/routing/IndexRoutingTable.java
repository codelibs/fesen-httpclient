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

import org.apache.lucene.util.CollectionUtil;
import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.RemoteStoreRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.codelibs.fesen.opensearch.common.Randomness;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.VerifiableWriteable;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The {@link IndexRoutingTable} represents routing information for a single
 * index. The routing table maintains a list of all shards in the index. A
 * single shard in this context has one more instances namely exactly one
 * {@link ShardRouting#primary() primary} and 1 or more replicas. In other
 * words, each instance of a shard is considered a replica while only one
 * replica per shard is a {@code primary} replica. The {@code primary} replica
 * can be seen as the "leader" of the shard acting as the primary entry point
 * for operations on a specific shard.
 * <p>
 * Note: The term replica is not directly
 * reflected in the routing table or in related classes, replicas are
 * represented as {@link ShardRouting}.
 * </p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexRoutingTable extends AbstractDiffable<IndexRoutingTable>
    implements
        Iterable<IndexShardRoutingTable>,
        VerifiableWriteable {

    private final Index index;
    private final ShardShuffler shuffler;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    private final Map<Integer, IndexShardRoutingTable> shards;

    private final List<ShardRouting> allActiveShards;

    IndexRoutingTable(Index index, final Map<Integer, IndexShardRoutingTable> shards) {
        this.index = index;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableMap(shards);
        List<ShardRouting> allActiveShards = new ArrayList<>();
        for (IndexShardRoutingTable cursor : shards.values()) {
            for (ShardRouting shardRouting : cursor) {
                if (shardRouting.active()) {
                    allActiveShards.add(shardRouting);
                }
            }
        }
        this.allActiveShards = Collections.unmodifiableList(allActiveShards);
    }

    /**
     * Return the index id
     *
     * @return id of the index
     */
    public Index getIndex() {
        return index;
    }

    @Override
    public Iterator<IndexShardRoutingTable> iterator() {
        return shards.values().iterator();
    }

    public Map<Integer, IndexShardRoutingTable> shards() {
        return shards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRoutingTable that = (IndexRoutingTable) o;

        if (!index.equals(that.index)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "IndexRoutingTable{" + "shards=" + shards + ", index=" + index + '}';
    }

    public static IndexRoutingTable readFrom(StreamInput in) throws IOException {
        Index index = new Index(in);
        Builder builder = new Builder(index);

        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
        }

        return builder.build();
    }

    public static Diff<IndexRoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexRoutingTable::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shards.size());
        for (IndexShardRoutingTable indexShard : this) {
            IndexShardRoutingTable.Builder.writeToThin(indexShard, out);
        }
    }

    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeMapValues(shards, (stream, value) -> IndexShardRoutingTable.Builder.writeVerifiableTo(value, stream));
    }

    /**
     * Builder of a routing table.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Index index;
        private final Map<Integer, IndexShardRoutingTable> shards = new HashMap<>();

        public Builder(Index index) {
            this.index = index;
        }

        /**
         * Initializes an existing index.
         */

        /**
         * Initializes a new empty index, with an option to control if its from an API or not.
         */

        public Builder addIndexShard(IndexShardRoutingTable indexShard) {
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public IndexRoutingTable build() {
            return new IndexRoutingTable(index, shards);
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- index [" + index + "]\n");

        List<IndexShardRoutingTable> ordered = new ArrayList<>();
        for (IndexShardRoutingTable indexShard : this) {
            ordered.add(indexShard);
        }

        CollectionUtil.timSort(ordered, (o1, o2) -> {
            int v = o1.shardId().getIndex().getName().compareTo(o2.shardId().getIndex().getName());
            if (v == 0) {
                v = Integer.compare(o1.shardId().id(), o2.shardId().id());
            }
            return v;
        });

        for (IndexShardRoutingTable indexShard : ordered) {
            sb.append("----shard_id [")
                .append(indexShard.shardId().getIndex().getName())
                .append("][")
                .append(indexShard.shardId().id())
                .append("]\n");
            for (ShardRouting shard : indexShard) {
                sb.append("--------").append(shard.shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }

}
