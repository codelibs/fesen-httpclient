/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Represents the hash range assigned to a shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record ShardRange(int shardId, int start, int end) implements Comparable<ShardRange>, ToXContentFragment, Writeable {

    /**
     * Constructs a new shard range from a stream.
     * @param in the stream to read from
     * @throws IOException if an error occurs while reading from the stream
     * @see #writeTo(StreamOutput)
     */
    public ShardRange(StreamInput in) throws IOException {
        this(in.readVInt(), in.readInt(), in.readInt());
    }

    @Override
    public int compareTo(ShardRange o) {
        return Integer.compare(start, o.start);
    }

    @Override
    public String toString() {
        return "ShardRange{" + "shardId=" + shardId + ", start=" + start + ", end=" + end + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeInt(start);
        out.writeInt(end);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("shard_id", shardId).field("start", start).field("end", end);
        builder.endObject();
        return builder;
    }
}
