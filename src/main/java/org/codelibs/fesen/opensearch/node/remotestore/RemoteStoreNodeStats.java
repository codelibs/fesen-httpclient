/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.node.remotestore;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Node level remote store stats
 * @opensearch.internal
 */
public class RemoteStoreNodeStats implements Writeable, ToXContentFragment {

    /**
     * The STATS_NAME constant.
     */
    public static final String STATS_NAME = "remote_store";
    /**
     * The LAST_SUCCESSFUL_FETCH_OF_PINNED_TIMESTAMPS constant.
     */
    public static final String LAST_SUCCESSFUL_FETCH_OF_PINNED_TIMESTAMPS = "last_successful_fetch_of_pinned_timestamps";

    /**
     * Time stamp for the last successful fetch of pinned timestamps by the node-side pinned-timestamp service
     */
    private final long lastSuccessfulFetchOfPinnedTimestamps;

    /**
     * Creates a new RemoteStoreNodeStats.
     */
    public RemoteStoreNodeStats() {
        this.lastSuccessfulFetchOfPinnedTimestamps = 0L;
    }

    /**
     * Returns the last successful fetch of pinned timestamps.
     *
     * @return the last successful fetch of pinned timestamps
     */
    public long getLastSuccessfulFetchOfPinnedTimestamps() {
        return this.lastSuccessfulFetchOfPinnedTimestamps;
    }

    /**
     * Creates a new RemoteStoreNodeStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public RemoteStoreNodeStats(StreamInput in) throws IOException {
        this.lastSuccessfulFetchOfPinnedTimestamps = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.lastSuccessfulFetchOfPinnedTimestamps);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(STATS_NAME);
        builder.field(LAST_SUCCESSFUL_FETCH_OF_PINNED_TIMESTAMPS, this.lastSuccessfulFetchOfPinnedTimestamps);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "RemoteStoreNodeStats{ lastSuccessfulFetchOfPinnedTimestamps=" + lastSuccessfulFetchOfPinnedTimestamps + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != RemoteStoreNodeStats.class) {
            return false;
        }
        RemoteStoreNodeStats other = (RemoteStoreNodeStats) o;
        return this.lastSuccessfulFetchOfPinnedTimestamps == other.lastSuccessfulFetchOfPinnedTimestamps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastSuccessfulFetchOfPinnedTimestamps);
    }
}
