/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Segment Replication Stats.
 *
 * @opensearch.internal
 */
public class SegmentReplicationStats implements Writeable, ToXContentFragment {

    private final Map<ShardId, SegmentReplicationPerGroupStats> shardStats;

    public SegmentReplicationStats(final Map<ShardId, SegmentReplicationPerGroupStats> shardStats) {
        this.shardStats = shardStats;
    }

    public SegmentReplicationStats(StreamInput in) throws IOException {
        int shardEntries = in.readInt();
        shardStats = new HashMap<>();
        for (int i = 0; i < shardEntries; i++) {
            ShardId shardId = new ShardId(in);
            SegmentReplicationPerGroupStats groupStats = new SegmentReplicationPerGroupStats(in);
            shardStats.put(shardId, groupStats);
        }
    }

    public Map<ShardId, SegmentReplicationPerGroupStats> getShardStats() {
        return shardStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("segment_replication");
        for (Map.Entry<ShardId, SegmentReplicationPerGroupStats> entry : shardStats.entrySet()) {
            builder.startObject(entry.getKey().toString());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardStats.size());
        for (Map.Entry<ShardId, SegmentReplicationPerGroupStats> entry : shardStats.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public String toString() {
        return "SegmentReplicationStats{" + "shardStats=" + shardStats + '}';
    }
}
