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

import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.cluster.Diffable;
import org.codelibs.fesen.opensearch.cluster.DiffableUtils;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.RemoteStoreRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.VerifiableWriteable;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.index.IndexNotFoundException;
import org.codelibs.fesen.opensearch.index.shard.ShardNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents a global cluster-wide routing table for all indices including the
 * version of the current routing state.
 *
 * @see IndexRoutingTable
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RoutingTable implements Iterable<IndexRoutingTable>, Diffable<RoutingTable>, VerifiableWriteable {

    public static final RoutingTable EMPTY_ROUTING_TABLE = builder().build();

    private final long version;

    // index to IndexRoutingTable map
    private final Map<String, IndexRoutingTable> indicesRouting;

    public RoutingTable(long version, final Map<String, IndexRoutingTable> indicesRouting) {
        this.version = version;
        this.indicesRouting = Collections.unmodifiableMap(indicesRouting);
    }

    @Override
    public Iterator<IndexRoutingTable> iterator() {
        return indicesRouting.values().iterator();
    }

    public IndexRoutingTable index(String index) {
        return indicesRouting.get(index);
    }

    public Map<String, IndexRoutingTable> indicesRouting() {
        return indicesRouting;
    }

    /**
     * All the shards (replicas) for all indices in this routing table.
     *
     * @return All the shards
     */
    public List<ShardRouting> allShards() {
        List<ShardRouting> shards = new ArrayList<>();
        String[] indices = indicesRouting.keySet().toArray(new String[0]);
        for (String index : indices) {
            List<ShardRouting> allShardsIndex = allShards(index);
            shards.addAll(allShardsIndex);
        }
        return shards;
    }

    /**
     * All the shards (replicas) for the provided index.
     *
     * @param index The index to return all the shards (replicas).
     * @return All the shards matching the specific index
     * @throws IndexNotFoundException If the index passed does not exists
     */
    public List<ShardRouting> allShards(String index) {
        List<ShardRouting> shards = new ArrayList<>();
        IndexRoutingTable indexRoutingTable = index(index);
        if (indexRoutingTable == null) {
            throw new IndexNotFoundException(index);
        }
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                shards.add(shardRouting);
            }
        }
        return shards;
    }

    private static Predicate<ShardRouting> ACTIVE_PREDICATE = ShardRouting::active;
    private static Predicate<ShardRouting> ASSIGNED_PREDICATE = ShardRouting::assignedToNode;

    @Override
    public Diff<RoutingTable> diff(RoutingTable previousState) {
        return new RoutingTableDiff(previousState, this);
    }

    public static Diff<RoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return new RoutingTableDiff(in);
    }

    public static RoutingTable readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexRoutingTable index = IndexRoutingTable.readFrom(in);
            builder.add(index);
        }

        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeVInt(indicesRouting.size());
        for (final IndexRoutingTable index : indicesRouting.values()) {
            index.writeTo(out);
        }
    }

    @Override
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeMapValues(indicesRouting, (stream, value) -> value.writeVerifiableTo((BufferedChecksumStreamOutput) stream));
    }

    private static class RoutingTableDiff implements Diff<RoutingTable>, StringKeyDiffProvider<IndexRoutingTable> {

        private final long version;

        private final Diff<Map<String, IndexRoutingTable>> indicesRouting;

        RoutingTableDiff(RoutingTable before, RoutingTable after) {
            version = after.version;
            indicesRouting = DiffableUtils.diff(before.indicesRouting, after.indicesRouting, DiffableUtils.getStringKeySerializer());
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexRoutingTable> DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexRoutingTable::readFrom, IndexRoutingTable::readDiffFrom);

        RoutingTableDiff(StreamInput in) throws IOException {
            version = in.readLong();
            indicesRouting = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), DIFF_VALUE_READER);
        }

        @Override
        public String toString() {
            return "RoutingTableDiff{" + "version=" + version + ", indicesRouting=" + indicesRouting + '}';
        }

        @Override
        public RoutingTable apply(RoutingTable part) {
            return new RoutingTable(version, indicesRouting.apply(part.indicesRouting));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            indicesRouting.writeTo(out);
        }

        @Override
        public DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> provideDiff() {
            return (DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>>) indicesRouting;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the routing table. Note that build can only be called one time.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private long version;
        private Map<String, IndexRoutingTable> indicesRouting = new HashMap<>();

        public Builder() {

        }

        public Builder add(IndexRoutingTable indexRoutingTable) {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            indicesRouting.put(indexRoutingTable.getIndex().getName(), indexRoutingTable);
            return this;
        }

        /**
         * Builds the routing table. Note that once this is called the builder
         * must be thrown away. If you need to build a new RoutingTable as a
         * copy of this one you'll need to build a new RoutingTable.Builder.
         */
        public RoutingTable build() {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            RoutingTable table = new RoutingTable(version, indicesRouting);
            indicesRouting = null;
            return table;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("routing_table (version ").append(version).append("):\n");
        for (final IndexRoutingTable entry : indicesRouting.values()) {
            sb.append(entry.prettyPrint()).append('\n');
        }
        return sb.toString();
    }

}
