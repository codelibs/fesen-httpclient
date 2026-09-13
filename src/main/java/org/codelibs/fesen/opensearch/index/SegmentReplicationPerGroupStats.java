/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

/**
 * Return Segment Replication stats for a Replication Group.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public class SegmentReplicationPerGroupStats implements Writeable, ToXContentFragment {

    private final ShardId shardId;
    private final Set<SegmentReplicationShardStats> replicaStats;
    private final long rejectedRequestCount;

    public SegmentReplicationPerGroupStats(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.replicaStats = in.readSet(SegmentReplicationShardStats::new);
        this.rejectedRequestCount = in.readVLong();
    }

    public Set<SegmentReplicationShardStats> getReplicaStats() {
        return replicaStats;
    }

    public long getRejectedRequestCount() {
        return rejectedRequestCount;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("rejected_requests", rejectedRequestCount);
        builder.startArray("replicas");
        for (SegmentReplicationShardStats stats : replicaStats) {
            stats.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeCollection(replicaStats);
        out.writeVLong(rejectedRequestCount);
    }

    @Override
    public String toString() {
        return "SegmentReplicationPerGroupStats{" + "replicaStats=" + replicaStats + ", rejectedRequestCount=" + rejectedRequestCount + '}';
    }
}
