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

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * indexRoutings like id, state, version, etc.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardRouting implements Writeable, ToXContentObject {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;

    private final ShardId shardId;
    private final String currentNodeId;
    private final String relocatingNodeId;
    private final boolean primary;
    private final boolean searchOnly;
    private final ShardRoutingState state;
    private final RecoverySource recoverySource;
    private final UnassignedInfo unassignedInfo;
    private final AllocationId allocationId;
    private final transient List<ShardRouting> asList;
    private final long expectedShardSize;
    @Nullable
    private final ShardRouting targetRelocatingShard;
    @Nullable
    private final ShardRouting[] recoveringChildShards;
    @Nullable
    private final ShardId parentShardId;

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    protected ShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        boolean searchOnly,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo,
        AllocationId allocationId,
        long expectedShardSize
    ) {
        this(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            searchOnly,
            state,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            null,
            null
        );
    }

    protected ShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        boolean searchOnly,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo,
        AllocationId allocationId,
        long expectedShardSize,
        ShardRouting[] recoveringChildShards,
        ShardId parentShardId
    ) {
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.searchOnly = searchOnly;
        this.state = state;
        this.recoverySource = recoverySource;
        this.unassignedInfo = unassignedInfo;
        this.allocationId = allocationId;
        this.expectedShardSize = expectedShardSize;
        this.targetRelocatingShard = initializeTargetRelocatingShard();
        this.recoveringChildShards = recoveringChildShards;
        this.parentShardId = parentShardId;
        this.asList = Collections.singletonList(this);
        assert expectedShardSize == UNAVAILABLE_EXPECTED_SHARD_SIZE
            || state == ShardRoutingState.INITIALIZING
            || state == ShardRoutingState.RELOCATING
            || state == ShardRoutingState.SPLITTING : expectedShardSize + " state: " + state;
        assert expectedShardSize >= 0
            || state != ShardRoutingState.INITIALIZING
            || state != ShardRoutingState.RELOCATING
            || state != ShardRoutingState.SPLITTING : expectedShardSize + " state: " + state;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
        assert (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) == (recoverySource != null)
            : "recovery source only available on unassigned or initializing shard but was " + state;
        assert recoverySource == null
            || recoverySource == PeerRecoverySource.INSTANCE
            || primary
            || searchOnly
            || recoverySource == RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE : "replica shards always recover from primary";
        assert (currentNodeId == null) == (state == ShardRoutingState.UNASSIGNED) : "unassigned shard must not be assigned to a node "
            + this;
    }

    @Nullable
    private ShardRouting initializeTargetRelocatingShard() {
        if (state == ShardRoutingState.RELOCATING) {
            return new ShardRouting(
                shardId,
                relocatingNodeId,
                currentNodeId,
                primary,
                searchOnly,
                ShardRoutingState.INITIALIZING,
                isSearchOnly() ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE,
                unassignedInfo,
                AllocationId.newTargetRelocation(allocationId),
                expectedShardSize
            );
        } else {
            return null;
        }
    }

    /**
     * Creates a new unassigned shard.
     */
    public static ShardRouting newUnassigned(
        ShardId shardId,
        boolean primary,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo
    ) {
        return newUnassigned(shardId, primary, false, recoverySource, unassignedInfo);
    }

    /**
     * Creates a new unassigned shard, overloaded for bwc for searchOnly addition.
     */
    public static ShardRouting newUnassigned(
        ShardId shardId,
        boolean primary,
        boolean search,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo
    ) {
        return new ShardRouting(
            shardId,
            null,
            null,
            primary,
            search,
            ShardRoutingState.UNASSIGNED,
            recoverySource,
            unassignedInfo,
            null,
            UNAVAILABLE_EXPECTED_SHARD_SIZE
        );
    }

    public Index index() {
        return shardId.getIndex();
    }

    /**
     * The index name.
     */
    public String getIndexName() {
        return shardId.getIndexName();
    }

    /**
     * The shard id.
     */
    public int id() {
        return shardId.id();
    }

    /**
     * The shard id.
     */
    public int getId() {
        return id();
    }

    /**
     * The shard is unassigned (not allocated to any node).
     */
    public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently
     * {@link ShardRoutingState#STARTED started} or
     * {@link ShardRoutingState#SPLITTING splitting} or
     * {@link ShardRoutingState#RELOCATING relocating} to another node.
     * Otherwise <code>false</code>
     */
    public boolean active() {
        return started() || relocating() || splitting();
    }

    /**
     * The shard is in started mode.
     */
    public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    /**
     * Returns <code>true</code> iff the shard is in splitting state.
     */
    public boolean splitting() {
        return state == ShardRoutingState.SPLITTING;
    }

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    public boolean assignedToNode() {
        return currentNodeId != null;
    }

    /**
     * The current node id the shard is allocated on.
     */
    public String currentNodeId() {
        return this.currentNodeId;
    }

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    /**
     * Returns a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting getTargetRelocatingShard() {
        assert relocating();
        return targetRelocatingShard;
    }

    /**
     * Additional metadata on why the shard is/was unassigned. The metadata is kept around
     * until the shard moves to STARTED.
     */
    @Nullable
    public UnassignedInfo unassignedInfo() {
        return unassignedInfo;
    }

    /**
     * An id that uniquely identifies an allocation.
     */
    @Nullable
    public AllocationId allocationId() {
        return this.allocationId;
    }

    /**
     * Returns <code>true</code> iff this shard is a primary.
     */
    public boolean primary() {
        return this.primary;
    }

    /**
     * Returns <code>true</code> iff this shard is a search only replica.
     */
    public boolean isSearchOnly() {
        return searchOnly;
    }

    /**
     * The shard state.
     */
    public ShardRoutingState state() {
        return this.state;
    }

    /**
     * The shard id.
     */
    public ShardId shardId() {
        return shardId;
    }

    public ShardRouting(ShardId shardId, StreamInput in) throws IOException {
        this.shardId = shardId;
        currentNodeId = in.readOptionalString();
        relocatingNodeId = in.readOptionalString();
        primary = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            searchOnly = in.readBoolean();
        } else {
            searchOnly = false;
        }
        state = ShardRoutingState.fromValue(in.readByte());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource = RecoverySource.readFrom(in);
        } else {
            recoverySource = null;
        }
        unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        allocationId = in.readOptionalWriteable(AllocationId::new);
        final long shardSize;
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.SPLITTING) {
            shardSize = in.readLong();
        } else {
            shardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
        expectedShardSize = shardSize;
        asList = Collections.singletonList(this);
        targetRelocatingShard = initializeTargetRelocatingShard();
        // These fields are transient - populated by RoutingNodes constructor, not serialized on the wire.
        recoveringChildShards = null;
        parentShardId = null;
    }

    public ShardRouting(StreamInput in) throws IOException {
        this(new ShardId(in), in);
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeOptionalString(currentNodeId);
        out.writeOptionalString(relocatingNodeId);
        out.writeBoolean(primary);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeBoolean(searchOnly);
        }
        out.writeByte(state.value());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource.writeTo(out);
        }
        out.writeOptionalWriteable(unassignedInfo);
        out.writeOptionalWriteable(allocationId);
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.SPLITTING) {
            out.writeLong(expectedShardSize);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        writeToThin(out);
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., unassigned info */
    public boolean equalsIgnoringMetadata(ShardRouting other) {
        if (primary != other.primary) {
            return false;
        }
        if (shardId != null ? !shardId.equals(other.shardId) : other.shardId != null) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(other.currentNodeId) : other.currentNodeId != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(other.relocatingNodeId) : other.relocatingNodeId != null) {
            return false;
        }
        if (allocationId != null ? !allocationId.equals(other.allocationId) : other.allocationId != null) {
            return false;
        }
        if (state != other.state) {
            return false;
        }
        if (recoverySource != null ? !recoverySource.equals(other.recoverySource) : other.recoverySource != null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        if (unassignedInfo != null ? !unassignedInfo.equals(that.unassignedInfo) : that.unassignedInfo != null) {
            return false;
        }
        return equalsIgnoringMetadata(that);
    }

    /**
     * Cache hash code in the same way as {@link String#hashCode()}) using racy single-check idiom
     * as it is mainly used in single-threaded code (the shard allocator).
     */
    private int hashCode; // default to 0

    @Override
    public int hashCode() {
        int h = hashCode;
        if (h == 0) {
            h = shardId.hashCode();
            h = 31 * h + (currentNodeId != null ? currentNodeId.hashCode() : 0);
            h = 31 * h + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
            h = 31 * h + (primary ? 1 : 0);
            h = 31 * h + (state != null ? state.hashCode() : 0);
            h = 31 * h + (recoverySource != null ? recoverySource.hashCode() : 0);
            h = 31 * h + (allocationId != null ? allocationId.hashCode() : 0);
            h = 31 * h + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
            hashCode = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    /**
     * A short description of the shard.
     */
    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(shardId.getIndexName()).append(']').append('[').append(shardId.getId()).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            if (searchOnly) {
                sb.append("[S]");
            } else {
                sb.append("[R]");
            }
        }
        if (recoverySource != null) {
            sb.append(", recovery_source[").append(recoverySource).append("]");
        }
        sb.append(", s[").append(state).append("]");
        if (allocationId != null) {
            sb.append(", a").append(allocationId);
        }
        if (this.unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo.toString());
        }
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            sb.append(", expected_shard_size[").append(expectedShardSize).append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder fieldBuilder = builder.startObject().field("state", state()).field("primary", primary());
        fieldBuilder.field("searchOnly", isSearchOnly());
        fieldBuilder.field("node", currentNodeId())
            .field("relocating_node", relocatingNodeId())
            .field("shard", id())
            .field("index", getIndexName());
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        if (recoverySource != null) {
            builder.field("recovery_source", recoverySource);
        }
        if (allocationId != null) {
            builder.field("allocation_id");
            allocationId.toXContent(builder, params);
        }
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        return builder.endObject();
    }

    /**
     * Returns recovery source for the given shard. Replica shards always recover from the primary {@link PeerRecoverySource}.
     *
     * @return recovery source or null if shard is {@link #active()}
     */
    @Nullable
    public RecoverySource recoverySource() {
        return recoverySource;
    }

    public boolean unassignedReasonIndexCreated() {
        if (unassignedInfo != null) {
            return unassignedInfo.getReason() == UnassignedInfo.Reason.INDEX_CREATED;
        }
        return false;
    }
}
