/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata for tracking shard split operations on an index.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SplitShardsMetadata extends AbstractDiffable<SplitShardsMetadata> implements ToXContentFragment {
    private static final int MINIMUM_RANGE_LENGTH_THRESHOLD = 1000;

    private static final String KEY_ROOT_SHARDS_TO_ALL_CHILDREN = "root_shards_to_all_children";
    private static final String KEY_NUMBER_OF_ROOT_SHARDS = "num_of_root_shards";
    private static final String KEY_PARENT_TO_CHILD_SHARDS = "parent_to_child_shards";
    private static final String KEY_MAX_SHARD_ID = "max_shard_id";
    private static final String KEY_IN_PROGRESS_SPLIT_SHARD_IDS = "in_progress_split_shard_id";
    private static final String KEY_ACTIVE_SHARD_IDS = "active_shard_ids";

    // Following fields are upadated only after split completion and are used to service active shards request.
    // Root shard id to flat list of all child shards under root.
    private final ShardRange[][] rootShardsToAllChildren;
    private final int maxShardId;
    private final Set<Integer> activeShardIds;

    // Following fields can store temporary information about in progress child shards along with info about
    // split completed shards.
    // Mapping of a parent shard ID to children.
    private final Map<Integer, ShardRange[]> parentToChildShards;
    private final Set<Integer> inProgressSplitShardIds;

    SplitShardsMetadata(
        ShardRange[][] rootShardsToAllChildren,
        Map<Integer, ShardRange[]> parentToChildShards,
        Set<Integer> inProgressSplitShardIds,
        Set<Integer> activeShardIds,
        int maxShardId
    ) {

        this.rootShardsToAllChildren = rootShardsToAllChildren;
        this.parentToChildShards = Collections.unmodifiableMap(parentToChildShards);
        this.maxShardId = maxShardId;
        this.inProgressSplitShardIds = Collections.unmodifiableSet(inProgressSplitShardIds);
        this.activeShardIds = activeShardIds;
    }

    public SplitShardsMetadata(StreamInput in) throws IOException {
        int numberOfRootShards = in.readVInt();
        this.rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
        for (int i = 0; i < numberOfRootShards; i++) {
            this.rootShardsToAllChildren[i] = in.readOptionalArray(ShardRange::new, ShardRange[]::new);
        }
        this.maxShardId = in.readVInt();
        this.inProgressSplitShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.activeShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.parentToChildShards = Collections.unmodifiableMap(
            in.readMap(StreamInput::readInt, i -> i.readArray(ShardRange::new, ShardRange[]::new))
        );
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rootShardsToAllChildren.length);
        for (ShardRange[] rootShardsToAllChild : rootShardsToAllChildren) {
            out.writeOptionalArray(rootShardsToAllChild);
        }
        out.writeVInt(this.maxShardId);
        out.writeCollection(this.inProgressSplitShardIds, StreamOutput::writeInt);
        out.writeCollection(this.activeShardIds, StreamOutput::writeInt);
        out.writeMap(this.parentToChildShards, StreamOutput::writeInt, StreamOutput::writeArray);
    }

    @Override
    public String toString() {
        StringBuilder parentToChildMap = new StringBuilder();
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            parentToChildMap.append("[");
            parentToChildMap.append(entry.getKey()).append("=").append(Arrays.toString(entry.getValue()));
            parentToChildMap.append("]");
        }
        return "SplitShardsMetadata{"
            + "rootShardsToAllChildren="
            + Arrays.toString(rootShardsToAllChildren)
            + ", maxShardId="
            + maxShardId
            + ", activeShardIds="
            + activeShardIds
            + ", parentToChildShards="
            + parentToChildMap
            + ", inProgressSplitShardIds="
            + inProgressSplitShardIds
            + '}';
    }

    public ShardRange[] getChildShardsOfParent(int shardId) {
        if (parentToChildShards.containsKey(shardId) == false) {
            return null;
        }

        ShardRange[] childShards = new ShardRange[parentToChildShards.get(shardId).length];
        int childShardIdx = 0;
        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShards[childShardIdx++] = childShard;
        }
        return childShards;
    }

    /**
     * Builder for {@link SplitShardsMetadata}.
     *
     * @opensearch.experimental
     */
    public static class Builder {
        private final ShardRange[][] rootShardsToAllChildren;
        private final Map<Integer, ShardRange[]> parentToChildShards;
        private int maxShardId;
        private final Set<Integer> inProgressSplitShardIds;
        private final Set<Integer> activeShardIds;

        public Builder(int numberOfShards) {
            maxShardId = numberOfShards - 1;
            rootShardsToAllChildren = new ShardRange[numberOfShards][];
            parentToChildShards = new HashMap<>();
            inProgressSplitShardIds = new HashSet<>();
            activeShardIds = new HashSet<>();
            for (int i = 0; i < numberOfShards; i++) {
                activeShardIds.add(i);
            }
        }

        public SplitShardsMetadata build() {
            return new SplitShardsMetadata(
                this.rootShardsToAllChildren,
                this.parentToChildShards,
                this.inProgressSplitShardIds,
                this.activeShardIds,
                this.maxShardId
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardsMetadata)) return false;

        SplitShardsMetadata that = (SplitShardsMetadata) o;

        if (maxShardId != that.maxShardId) return false;
        if (!inProgressSplitShardIds.equals(that.inProgressSplitShardIds)) return false;
        if (!Arrays.deepEquals(rootShardsToAllChildren, that.rootShardsToAllChildren)) return false;
        if (!activeShardIds.equals(that.activeShardIds)) return false;
        if (parentToChildShards.size() != that.parentToChildShards.size()) return false;
        for (Integer key : parentToChildShards.keySet()) {
            if (!Arrays.deepEquals(parentToChildShards.get(key), that.parentToChildShards.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(rootShardsToAllChildren);
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            result = 31 * result + Objects.hash(entry.getKey(), Arrays.deepHashCode(entry.getValue()));
        }
        result = 31 * result + maxShardId;
        result = 31 * result + inProgressSplitShardIds.hashCode();
        result = 31 * result + activeShardIds.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_NUMBER_OF_ROOT_SHARDS, rootShardsToAllChildren.length);
        builder.field(KEY_MAX_SHARD_ID, maxShardId);
        if (!inProgressSplitShardIds.isEmpty()) {
            builder.field(KEY_IN_PROGRESS_SPLIT_SHARD_IDS, new ArrayList<>(inProgressSplitShardIds));
        }
        builder.field(KEY_ACTIVE_SHARD_IDS, new ArrayList<>(activeShardIds));
        builder.startObject(KEY_ROOT_SHARDS_TO_ALL_CHILDREN);
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            ShardRange[] childShards = rootShardsToAllChildren[rootShardId];
            if (childShards != null) {
                builder.startArray(String.valueOf(rootShardId));
                for (ShardRange childShard : childShards) {
                    childShard.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        builder.endObject();

        builder.startObject(KEY_PARENT_TO_CHILD_SHARDS);
        for (Integer parentShardId : parentToChildShards.keySet()) {
            builder.startArray(String.valueOf(parentShardId));
            for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
                childShard.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    public static Diff<SplitShardsMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardsMetadata::new, in);
    }

}
