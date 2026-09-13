/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.common.CheckedFunction;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.Loggers;
import org.codelibs.fesen.opensearch.common.util.Streak;
import org.codelibs.fesen.opensearch.common.util.concurrent.ConcurrentCollections;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.index.store.DirectoryFileTransferTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * Keeps track of remote refresh which happens in {@link org.codelibs.fesen.opensearch.index.shard.RemoteStoreRefreshListener}. This consist of multiple critical metrics.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
public class RemoteSegmentTransferTracker extends RemoteTransferTracker {

    private final Logger logger;

    /**
     * The refresh time of the most recent refresh.
     */
    private volatile long localRefreshTimeMs;

    /**
     * The refresh time(clock) of the most recent refresh.
     */
    private volatile long localRefreshClockTimeMs;

    /**
     * The refresh time of the most recent remote refresh.
     */
    private volatile long remoteRefreshTimeMs;

    /**
     * This is the time of first local refresh after the last successful remote refresh. When the remote store is in
     * sync with local refresh, this will be reset to -1.
     */
    private volatile long remoteRefreshStartTimeMs = -1;

    /**
     * The refresh time(clock) of the most recent remote refresh.
     */
    private volatile long remoteRefreshClockTimeMs;

    /**
     * Cumulative sum of rejection counts for this shard.
     */
    private final AtomicLong rejectionCount = new AtomicLong();

    /**
     * Keeps track of rejection count with each rejection reason.
     */
    private final Map<String, AtomicLong> rejectionCountMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Keeps track of segment files and their size in bytes which are part of the most recent refresh.
     */
    private final Map<String, Long> latestLocalFileNameLengthMap = ConcurrentCollections.newConcurrentMap();

    /**
     * This contains the files from the last successful remote refresh and ongoing uploads. This gets reset to just the
     * last successful remote refresh state on successful remote refresh.
     */
    private final Set<String> latestUploadedFiles = ConcurrentCollections.newConcurrentSet();

    /**
     * Holds count of consecutive failures until last success. Gets reset to zero if there is a success.
     */
    private final Streak failures = new Streak();

    /**
     * {@link org.codelibs.fesen.opensearch.index.store.Store.StoreDirectory} level file transfer tracker, used to show download stats
     */
    private final DirectoryFileTransferTracker directoryFileTransferTracker;

    public RemoteSegmentTransferTracker(
        ShardId shardId,
        DirectoryFileTransferTracker directoryFileTransferTracker,
        int movingAverageWindowSize
    ) {
        super(shardId, movingAverageWindowSize);

        logger = Loggers.getLogger(getClass(), shardId);
        // Both the local refresh time and remote refresh time are set with current time to give consistent view of time lag when it arises.
        long currentClockTimeMs = System.currentTimeMillis();
        long currentTimeMs = currentTimeMsUsingSystemNanos();
        localRefreshTimeMs = currentTimeMs;
        remoteRefreshTimeMs = currentTimeMs;
        remoteRefreshStartTimeMs = currentTimeMs;
        localRefreshClockTimeMs = currentClockTimeMs;
        remoteRefreshClockTimeMs = currentClockTimeMs;
        this.directoryFileTransferTracker = directoryFileTransferTracker;
    }

    public static long currentTimeMsUsingSystemNanos() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public void incrementTotalUploadsFailed() {
        super.incrementTotalUploadsFailed();
        failures.record(true);
    }

    @Override
    public void incrementTotalUploadsSucceeded() {
        super.incrementTotalUploadsSucceeded();
        failures.record(false);
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.10.0")
    public static class Stats implements Writeable {

        public final ShardId shardId;
        public final long localRefreshClockTimeMs;
        public final long remoteRefreshClockTimeMs;
        public final long refreshTimeLagMs;
        public final long localRefreshNumber;
        public final long remoteRefreshNumber;
        public final long uploadBytesStarted;
        public final long uploadBytesFailed;
        public final long uploadBytesSucceeded;
        public final long totalUploadsStarted;
        public final long totalUploadsFailed;
        public final long totalUploadsSucceeded;
        public final long rejectionCount;
        public final long consecutiveFailuresCount;
        public final long lastSuccessfulRemoteRefreshBytes;
        public final double uploadBytesMovingAverage;
        public final double uploadBytesPerSecMovingAverage;
        public final long totalUploadTimeInMs;
        public final double uploadTimeMovingAverage;
        public final long bytesLag;
        public final DirectoryFileTransferTracker.Stats directoryFileTransferTrackerStats;

        private Stats(Builder builder) {
            this.shardId = builder.shardId;
            this.localRefreshClockTimeMs = builder.localRefreshClockTimeMs;
            this.remoteRefreshClockTimeMs = builder.remoteRefreshClockTimeMs;
            this.refreshTimeLagMs = builder.refreshTimeLagMs;
            this.localRefreshNumber = builder.localRefreshNumber;
            this.remoteRefreshNumber = builder.remoteRefreshNumber;
            this.uploadBytesStarted = builder.uploadBytesStarted;
            this.uploadBytesFailed = builder.uploadBytesFailed;
            this.uploadBytesSucceeded = builder.uploadBytesSucceeded;
            this.totalUploadsStarted = builder.totalUploadsStarted;
            this.totalUploadsFailed = builder.totalUploadsFailed;
            this.totalUploadsSucceeded = builder.totalUploadsSucceeded;
            this.rejectionCount = builder.rejectionCount;
            this.consecutiveFailuresCount = builder.consecutiveFailuresCount;
            this.lastSuccessfulRemoteRefreshBytes = builder.lastSuccessfulRemoteRefreshBytes;
            this.uploadBytesMovingAverage = builder.uploadBytesMovingAverage;
            this.uploadBytesPerSecMovingAverage = builder.uploadBytesPerSecMovingAverage;
            this.totalUploadTimeInMs = builder.totalUploadTimeInMs;
            this.uploadTimeMovingAverage = builder.uploadTimeMovingAverage;
            this.bytesLag = builder.bytesLag;
            this.directoryFileTransferTrackerStats = builder.directoryFileTransferTrackerStats;
        }

        public Stats(StreamInput in) throws IOException {
            try {
                this.shardId = new ShardId(in);
                this.localRefreshClockTimeMs = in.readLong();
                this.remoteRefreshClockTimeMs = in.readLong();
                this.refreshTimeLagMs = in.readLong();
                this.localRefreshNumber = in.readLong();
                this.remoteRefreshNumber = in.readLong();
                this.uploadBytesStarted = in.readLong();
                this.uploadBytesFailed = in.readLong();
                this.uploadBytesSucceeded = in.readLong();
                this.totalUploadsStarted = in.readLong();
                this.totalUploadsFailed = in.readLong();
                this.totalUploadsSucceeded = in.readLong();
                this.rejectionCount = in.readLong();
                this.consecutiveFailuresCount = in.readLong();
                this.lastSuccessfulRemoteRefreshBytes = in.readLong();
                this.uploadBytesMovingAverage = in.readDouble();
                this.uploadBytesPerSecMovingAverage = in.readDouble();
                this.uploadTimeMovingAverage = in.readDouble();
                this.bytesLag = in.readLong();
                this.totalUploadTimeInMs = in.readLong();
                this.directoryFileTransferTrackerStats = in.readOptionalWriteable(DirectoryFileTransferTracker.Stats::new);
            } catch (IOException e) {
                throw e;
            }
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private ShardId shardId = null;
            private long localRefreshClockTimeMs = 0;
            private long remoteRefreshClockTimeMs = 0;
            private long refreshTimeLagMs = 0;
            private long localRefreshNumber = 0;
            private long remoteRefreshNumber = 0;
            private long uploadBytesStarted = 0;
            private long uploadBytesFailed = 0;
            private long uploadBytesSucceeded = 0;
            private long totalUploadsStarted = 0;
            private long totalUploadsFailed = 0;
            private long totalUploadsSucceeded = 0;
            private long rejectionCount = 0;
            private long consecutiveFailuresCount = 0;
            private long lastSuccessfulRemoteRefreshBytes = 0;
            private double uploadBytesMovingAverage = 0;
            private double uploadBytesPerSecMovingAverage = 0;
            private long totalUploadTimeInMs = 0;
            private double uploadTimeMovingAverage = 0;
            private long bytesLag = 0;
            private DirectoryFileTransferTracker.Stats directoryFileTransferTrackerStats = null;

            public Builder() {}

            public Builder shardId(ShardId shardId) {
                this.shardId = shardId;
                return this;
            }

            public Builder localRefreshClockTimeMs(long time) {
                this.localRefreshClockTimeMs = time;
                return this;
            }

            public Builder remoteRefreshClockTimeMs(long time) {
                this.remoteRefreshClockTimeMs = time;
                return this;
            }

            public Builder refreshTimeLagMs(long time) {
                this.refreshTimeLagMs = time;
                return this;
            }

            public Builder localRefreshNumber(long number) {
                this.localRefreshNumber = number;
                return this;
            }

            public Builder remoteRefreshNumber(long number) {
                this.remoteRefreshNumber = number;
                return this;
            }

            public Builder uploadBytesStarted(long started) {
                this.uploadBytesStarted = started;
                return this;
            }

            public Builder uploadBytesFailed(long failed) {
                this.uploadBytesFailed = failed;
                return this;
            }

            public Builder uploadBytesSucceeded(long succeeded) {
                this.uploadBytesSucceeded = succeeded;
                return this;
            }

            public Builder totalUploadsStarted(long started) {
                this.totalUploadsStarted = started;
                return this;
            }

            public Builder totalUploadsFailed(long failed) {
                this.totalUploadsFailed = failed;
                return this;
            }

            public Builder totalUploadsSucceeded(long succeeded) {
                this.totalUploadsSucceeded = succeeded;
                return this;
            }

            public Builder rejectionCount(long count) {
                this.rejectionCount = count;
                return this;
            }

            public Builder consecutiveFailuresCount(long count) {
                this.consecutiveFailuresCount = count;
                return this;
            }

            public Builder lastSuccessfulRemoteRefreshBytes(long bytes) {
                this.lastSuccessfulRemoteRefreshBytes = bytes;
                return this;
            }

            public Builder uploadBytesMovingAverage(double average) {
                this.uploadBytesMovingAverage = average;
                return this;
            }

            public Builder uploadBytesPerSecMovingAverage(double average) {
                this.uploadBytesPerSecMovingAverage = average;
                return this;
            }

            public Builder totalUploadTimeInMs(long time) {
                this.totalUploadTimeInMs = time;
                return this;
            }

            public Builder uploadTimeMovingAverage(double average) {
                this.uploadTimeMovingAverage = average;
                return this;
            }

            public Builder bytesLag(long lag) {
                this.bytesLag = lag;
                return this;
            }

            public Builder directoryFileTransferTrackerStats(DirectoryFileTransferTracker.Stats stats) {
                this.directoryFileTransferTrackerStats = stats;
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
            shardId.writeTo(out);
            out.writeLong(localRefreshClockTimeMs);
            out.writeLong(remoteRefreshClockTimeMs);
            out.writeLong(refreshTimeLagMs);
            out.writeLong(localRefreshNumber);
            out.writeLong(remoteRefreshNumber);
            out.writeLong(uploadBytesStarted);
            out.writeLong(uploadBytesFailed);
            out.writeLong(uploadBytesSucceeded);
            out.writeLong(totalUploadsStarted);
            out.writeLong(totalUploadsFailed);
            out.writeLong(totalUploadsSucceeded);
            out.writeLong(rejectionCount);
            out.writeLong(consecutiveFailuresCount);
            out.writeLong(lastSuccessfulRemoteRefreshBytes);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);
            out.writeLong(bytesLag);
            out.writeLong(totalUploadTimeInMs);
            out.writeOptionalWriteable(directoryFileTransferTrackerStats);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Stats other = (Stats) obj;

            return this.shardId.toString().equals(other.shardId.toString())
                && this.localRefreshClockTimeMs == other.localRefreshClockTimeMs
                && this.remoteRefreshClockTimeMs == other.remoteRefreshClockTimeMs
                && this.refreshTimeLagMs == other.refreshTimeLagMs
                && this.localRefreshNumber == other.localRefreshNumber
                && this.remoteRefreshNumber == other.remoteRefreshNumber
                && this.uploadBytesStarted == other.uploadBytesStarted
                && this.uploadBytesFailed == other.uploadBytesFailed
                && this.uploadBytesSucceeded == other.uploadBytesSucceeded
                && this.totalUploadsStarted == other.totalUploadsStarted
                && this.totalUploadsFailed == other.totalUploadsFailed
                && this.totalUploadsSucceeded == other.totalUploadsSucceeded
                && this.rejectionCount == other.rejectionCount
                && this.consecutiveFailuresCount == other.consecutiveFailuresCount
                && this.lastSuccessfulRemoteRefreshBytes == other.lastSuccessfulRemoteRefreshBytes
                && Double.compare(this.uploadBytesMovingAverage, other.uploadBytesMovingAverage) == 0
                && Double.compare(this.uploadBytesPerSecMovingAverage, other.uploadBytesPerSecMovingAverage) == 0
                && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0
                && this.bytesLag == other.bytesLag
                && this.totalUploadTimeInMs == other.totalUploadTimeInMs
                && this.directoryFileTransferTrackerStats.equals(other.directoryFileTransferTrackerStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                shardId,
                localRefreshClockTimeMs,
                remoteRefreshClockTimeMs,
                refreshTimeLagMs,
                localRefreshNumber,
                remoteRefreshNumber,
                uploadBytesStarted,
                uploadBytesFailed,
                uploadBytesSucceeded,
                totalUploadsStarted,
                totalUploadsFailed,
                totalUploadsSucceeded,
                rejectionCount,
                consecutiveFailuresCount,
                lastSuccessfulRemoteRefreshBytes,
                uploadBytesMovingAverage,
                uploadBytesPerSecMovingAverage,
                uploadTimeMovingAverage,
                bytesLag,
                totalUploadTimeInMs,
                directoryFileTransferTrackerStats
            );
        }

        @Override
        public String toString() {
            return "Stats{"
                + "shardId="
                + shardId
                + ", localRefreshClockTimeMs="
                + localRefreshClockTimeMs
                + ", remoteRefreshClockTimeMs="
                + remoteRefreshClockTimeMs
                + ", refreshTimeLagMs="
                + refreshTimeLagMs
                + ", localRefreshNumber="
                + localRefreshNumber
                + ", remoteRefreshNumber="
                + remoteRefreshNumber
                + ", uploadBytesStarted="
                + uploadBytesStarted
                + ", uploadBytesFailed="
                + uploadBytesFailed
                + ", uploadBytesSucceeded="
                + uploadBytesSucceeded
                + ", totalUploadsStarted="
                + totalUploadsStarted
                + ", totalUploadsFailed="
                + totalUploadsFailed
                + ", totalUploadsSucceeded="
                + totalUploadsSucceeded
                + ", rejectionCount="
                + rejectionCount
                + ", consecutiveFailuresCount="
                + consecutiveFailuresCount
                + ", lastSuccessfulRemoteRefreshBytes="
                + lastSuccessfulRemoteRefreshBytes
                + ", uploadBytesMovingAverage="
                + uploadBytesMovingAverage
                + ", uploadBytesPerSecMovingAverage="
                + uploadBytesPerSecMovingAverage
                + ", totalUploadTimeInMs="
                + totalUploadTimeInMs
                + ", uploadTimeMovingAverage="
                + uploadTimeMovingAverage
                + ", bytesLag="
                + bytesLag
                + ", directoryFileTransferTrackerStats="
                + directoryFileTransferTrackerStats
                + '}';
        }
    }
}
