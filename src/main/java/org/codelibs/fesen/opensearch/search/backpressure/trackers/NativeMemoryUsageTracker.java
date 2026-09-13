/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.search.backpressure.trackers;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;

import java.io.IOException;
import java.util.Objects;

/**
 * Namespace for the native-memory tracker statistics a client reads back from node stats.
 *
 * <p>Tracking native memory is a node-side concern and is not carried over.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public final class NativeMemoryUsageTracker {

    private NativeMemoryUsageTracker() {
    }

    /**
     * Stats for {@link NativeMemoryUsageTracker}. Serialization shape deliberately omits the
     * rolling-average field that {@link HeapUsageTracker.Stats} carries: we don't maintain a
     * rolling baseline for native memory, so exposing a zeroed field would be misleading.
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;

        public Stats(long cancellationCount, long currentMax, long currentAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount && currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg);
        }

        @Override
        public String toString() {
            return "NativeMemoryUsageTracker.Stats{cancellationCount="
                + cancellationCount
                + ", currentMax="
                + currentMax
                + ", currentAvg="
                + currentAvg
                + "}";
        }
    }
}
