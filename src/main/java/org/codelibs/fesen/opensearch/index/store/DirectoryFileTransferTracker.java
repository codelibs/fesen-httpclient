/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.MovingAverage;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the amount of bytes transferred between two {@link Directory} instances
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
public class DirectoryFileTransferTracker {
    /**
     * Cumulative size of files (in bytes) attempted to be transferred over from the source {@link Directory}
     */
    private final AtomicLong transferredBytesStarted = new AtomicLong();

    /**
     * Cumulative size of files (in bytes) successfully transferred over from the source {@link Directory}
     */
    private final AtomicLong transferredBytesFailed = new AtomicLong();

    /**
     * Cumulative size of files (in bytes) failed in transfer over from the source {@link Directory}
     */
    private final AtomicLong transferredBytesSucceeded = new AtomicLong();

    /**
     * Time in milliseconds for the last successful transfer from the source {@link Directory}
     */
    private final AtomicLong lastTransferTimestampMs = new AtomicLong();

    /**
     * Cumulative time in milliseconds spent in successful transfers from the source {@link Directory}
     */
    private final AtomicLong totalTransferTimeInMs = new AtomicLong();

    /**
     * Provides moving average over the last N total size in bytes of files transferred from the source {@link Directory}.
     * N is window size
     */
    private final AtomicReference<MovingAverage> transferredBytesMovingAverageReference;

    private final AtomicLong lastSuccessfulTransferInBytes = new AtomicLong();

    /**
     * Provides moving average over the last N transfer speed (in bytes/s) of segment files transferred from the source {@link Directory}.
     * N is window size
     */
    private final AtomicReference<MovingAverage> transferredBytesPerSecMovingAverageReference;

    private final int DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE = 20;

    public DirectoryFileTransferTracker() {
        transferredBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE));
        transferredBytesPerSecMovingAverageReference = new AtomicReference<>(
            new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE)
        );
    }

    /**
     * Represents the tracker's stats presentable to an API.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.10.0")
    public static class Stats implements Writeable {
        public final long transferredBytesStarted;
        public final long transferredBytesFailed;
        public final long transferredBytesSucceeded;
        public final long lastTransferTimestampMs;
        public final long totalTransferTimeInMs;
        public final double transferredBytesMovingAverage;
        public final long lastSuccessfulTransferInBytes;
        public final double transferredBytesPerSecMovingAverage;

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder) {
            this.transferredBytesStarted = builder.transferredBytesStarted;
            this.transferredBytesFailed = builder.transferredBytesFailed;
            this.transferredBytesSucceeded = builder.transferredBytesSucceeded;
            this.lastTransferTimestampMs = builder.lastTransferTimestampMs;
            this.totalTransferTimeInMs = builder.totalTransferTimeInMs;
            this.transferredBytesMovingAverage = builder.transferredBytesMovingAverage;
            this.lastSuccessfulTransferInBytes = builder.lastSuccessfulTransferInBytes;
            this.transferredBytesPerSecMovingAverage = builder.transferredBytesPerSecMovingAverage;
        }

        public Stats(StreamInput in) throws IOException {
            this.transferredBytesStarted = in.readLong();
            this.transferredBytesFailed = in.readLong();
            this.transferredBytesSucceeded = in.readLong();
            this.lastTransferTimestampMs = in.readLong();
            this.totalTransferTimeInMs = in.readLong();
            this.transferredBytesMovingAverage = in.readDouble();
            this.lastSuccessfulTransferInBytes = in.readLong();
            this.transferredBytesPerSecMovingAverage = in.readDouble();
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private long transferredBytesStarted = 0;
            private long transferredBytesFailed = 0;
            private long transferredBytesSucceeded = 0;
            private long lastTransferTimestampMs = 0;
            private long totalTransferTimeInMs = 0;
            private double transferredBytesMovingAverage = 0;
            private long lastSuccessfulTransferInBytes = 0;
            private double transferredBytesPerSecMovingAverage = 0;

            public Builder() {}

            public Builder transferredBytesStarted(long started) {
                this.transferredBytesStarted = started;
                return this;
            }

            public Builder transferredBytesFailed(long failed) {
                this.transferredBytesFailed = failed;
                return this;
            }

            public Builder transferredBytesSucceeded(long succeeded) {
                this.transferredBytesSucceeded = succeeded;
                return this;
            }

            public Builder lastTransferTimestampMs(long timestamp) {
                this.lastTransferTimestampMs = timestamp;
                return this;
            }

            public Builder totalTransferTimeInMs(long time) {
                this.totalTransferTimeInMs = time;
                return this;
            }

            public Builder transferredBytesMovingAverage(double average) {
                this.transferredBytesMovingAverage = average;
                return this;
            }

            public Builder lastSuccessfulTransferInBytes(long bytes) {
                this.lastSuccessfulTransferInBytes = bytes;
                return this;
            }

            public Builder transferredBytesPerSecMovingAverage(double average) {
                this.transferredBytesPerSecMovingAverage = average;
                return this;
            }

            /**
             * Creates a {@link Stats} object from the builder's current state.
             * @return A new Stats instance.
             */
            public Stats build() {
                return new Stats(this);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(transferredBytesStarted);
            out.writeLong(transferredBytesFailed);
            out.writeLong(transferredBytesSucceeded);
            out.writeLong(lastTransferTimestampMs);
            out.writeLong(totalTransferTimeInMs);
            out.writeDouble(transferredBytesMovingAverage);
            out.writeLong(lastSuccessfulTransferInBytes);
            out.writeDouble(transferredBytesPerSecMovingAverage);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Stats stats = (Stats) obj;

            return transferredBytesStarted == stats.transferredBytesStarted
                && transferredBytesFailed == stats.transferredBytesFailed
                && transferredBytesSucceeded == stats.transferredBytesSucceeded
                && lastTransferTimestampMs == stats.lastTransferTimestampMs
                && totalTransferTimeInMs == stats.totalTransferTimeInMs
                && Double.compare(stats.transferredBytesMovingAverage, transferredBytesMovingAverage) == 0
                && lastSuccessfulTransferInBytes == stats.lastSuccessfulTransferInBytes
                && Double.compare(stats.transferredBytesPerSecMovingAverage, transferredBytesPerSecMovingAverage) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                transferredBytesStarted,
                transferredBytesFailed,
                transferredBytesSucceeded,
                lastTransferTimestampMs,
                totalTransferTimeInMs,
                transferredBytesMovingAverage,
                lastSuccessfulTransferInBytes,
                transferredBytesPerSecMovingAverage
            );
        }
    }
}
