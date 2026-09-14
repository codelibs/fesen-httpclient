/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.codelibs.fesen.opensearch.index;

import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for all the tracking that needs to be performed at every Shard Level for Indexing Operations on the node.
 * Info is maintained at the granularity of three kinds of write operation (tasks) on the node i.e. coordinating, primary and replica.
 * This is useful in evaluating the shard indexing back-pressure on the node, to throttle requests and also to publish runtime stats.
 * <p>
 * There can be four kinds of operation tracking on a node which needs to performed for a shard:
 * 1. Coordinating Operation : To track all the individual shard bulk request on the coordinator node.
 * 2. Primary Operation : To track all the individual shard bulk request on the primary node.
 * 3. Replica Operation : To track all the individual shard bulk request on the replica node.
 * 4. Common Operation : To track values applicable across the specific shard role.
 * <p>
 * ShardIndexingPressureTracker therefore provides the construct to track all the write requests targeted for a ShardId on the node,
 * across all possible transport-write-actions i.e. Coordinator, Primary and Replica.
 * Tracker is uniquely identified against a Shard-Id on the node. Currently the knowledge of shard roles (such as primary vs replica)
 * is not explicit to the tracker, and it is able to track different values simultaneously based on the interaction hooks of the
 * operation type i.e. write-action layers.
 * <p>
 * There is room for introducing more unique identity to the trackers based on Shard-Role or Shard-Allocation-Id, but that will also
 * increase the complexity of handling shard-lister events and handling other race scenarios such as request-draining etc.
 * To prefer simplicity we have modelled by keeping explicit fields for different operation tracking, while tracker by itself is
 * agnostic of the actual shard role.
 *
 * @opensearch.internal
 */
public class ShardIndexingPressureTracker {

    private final ShardId shardId;
    private final AtomicLong primaryAndCoordinatingLimits;
    private final AtomicLong replicaLimits;

    private final OperationTracker coordinatingOperationTracker = new OperationTracker();
    private final OperationTracker primaryOperationTracker = new OperationTracker();
    private final OperationTracker replicaOperationTracker = new OperationTracker();
    private final CommonOperationTracker commonOperationTracker = new CommonOperationTracker();

    /**
     * Creates a new ShardIndexingPressureTracker.
     *
     * @param shardId the shard identifier
     * @param initialPrimaryAndCoordinatingLimits the initial primary and coordinating limits
     * @param initialReplicaLimits the initial replica limits
     */
    public ShardIndexingPressureTracker(ShardId shardId, long initialPrimaryAndCoordinatingLimits, long initialReplicaLimits) {
        this.shardId = shardId;
        this.primaryAndCoordinatingLimits = new AtomicLong(initialPrimaryAndCoordinatingLimits);
        this.replicaLimits = new AtomicLong(initialReplicaLimits);
    }

    /**
     * Returns the shard identifier.
     *
     * @return the shard identifier
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns the primary and coordinating limits.
     *
     * @return the primary and coordinating limits
     */
    public long getPrimaryAndCoordinatingLimits() {
        return primaryAndCoordinatingLimits.get();
    }

    /**
     * Compares the and set primary and coordinating limits.
     *
     * @param expectedValue the expected value
     * @param newValue the new value
     * @return this instance
     */
    public boolean compareAndSetPrimaryAndCoordinatingLimits(long expectedValue, long newValue) {
        return primaryAndCoordinatingLimits.compareAndSet(expectedValue, newValue);
    }

    /**
     * Returns the replica limits.
     *
     * @return the replica limits
     */
    public long getReplicaLimits() {
        return replicaLimits.get();
    }

    /**
     * Compares the and set replica limits.
     *
     * @param expectedValue the expected value
     * @param newValue the new value
     * @return this instance
     */
    public boolean compareAndSetReplicaLimits(long expectedValue, long newValue) {
        return replicaLimits.compareAndSet(expectedValue, newValue);
    }

    /**
     * Returns the coordinating operation tracker.
     *
     * @return the coordinating operation tracker
     */
    public OperationTracker getCoordinatingOperationTracker() {
        return coordinatingOperationTracker;
    }

    /**
     * Returns the primary operation tracker.
     *
     * @return the primary operation tracker
     */
    public OperationTracker getPrimaryOperationTracker() {
        return primaryOperationTracker;
    }

    /**
     * Returns the replica operation tracker.
     *
     * @return the replica operation tracker
     */
    public OperationTracker getReplicaOperationTracker() {
        return replicaOperationTracker;
    }

    /**
     * Returns the common operation tracker.
     *
     * @return the common operation tracker
     */
    public CommonOperationTracker getCommonOperationTracker() {
        return commonOperationTracker;
    }

    /**
     * OperationTracker bundles the different kind of attributes which needs to be tracked for every operation, per shard:
     * a. StatsTracker : To track request level aggregated statistics for a shard
     * b. RejectionTracker : To track the rejection statistics for a shard
     * c. Performance Tracker : To track the request performance statistics for a shard
     *
     * @opensearch.internal
     */
    public static class OperationTracker {
        /**
         * Creates a new OperationTracker.
         */
        public OperationTracker() {
        }

        private final StatsTracker statsTracker = new StatsTracker();
        private final RejectionTracker rejectionTracker = new RejectionTracker();
        private final PerformanceTracker performanceTracker = new PerformanceTracker();

        /**
         * Returns the stats tracker.
         *
         * @return the stats tracker
         */
        public StatsTracker getStatsTracker() {
            return statsTracker;
        }

        /**
         * Returns the rejection tracker.
         *
         * @return the rejection tracker
         */
        public RejectionTracker getRejectionTracker() {
            return rejectionTracker;
        }

        /**
         * Returns the performance tracker.
         *
         * @return the performance tracker
         */
        public PerformanceTracker getPerformanceTracker() {
            return performanceTracker;
        }
    }

    /**
     * StatsTracker is used to track request level aggregated statistics for a shard. This includes:
     * a. currentBytes - Bytes of data that is inflight/processing for a shard.
     * b. totalBytes - Total bytes that are processed/completed successfully for a shard.
     * c. requestCount - Total number of requests that are processed/completed successfully for a shard.
     *
     * @opensearch.internal
     */
    public static class StatsTracker {
        /**
         * Creates a new StatsTracker.
         */
        public StatsTracker() {
        }

        private final AtomicLong currentBytes = new AtomicLong();
        private final AtomicLong totalBytes = new AtomicLong();
        private final AtomicLong requestCount = new AtomicLong();

        /**
         * Returns the current bytes.
         *
         * @return the current bytes
         */
        public long getCurrentBytes() {
            return currentBytes.get();
        }

        /**
         * Increments the current bytes.
         *
         * @param bytes the bytes
         * @return this instance
         */
        public long incrementCurrentBytes(long bytes) {
            return currentBytes.addAndGet(bytes);
        }

        /**
         * Returns the total bytes.
         *
         * @return the total bytes
         */
        public long getTotalBytes() {
            return totalBytes.get();
        }

        /**
         * Increments the total bytes.
         *
         * @param bytes the bytes
         * @return this instance
         */
        public long incrementTotalBytes(long bytes) {
            return totalBytes.addAndGet(bytes);
        }

        /**
         * Returns the request count.
         *
         * @return the request count
         */
        public long getRequestCount() {
            return requestCount.get();
        }

        /**
         * Increments the request count.
         *
         * @return this instance
         */
        public long incrementRequestCount() {
            return requestCount.incrementAndGet();
        }
    }

    /**
     * RejectionTracker allows tracking the rejection statistics per shard. This includes:
     * a. totalRejections - Total number of requests that were rejected for a shard.
     * b. nodeLimitsBreachedRejections - Total number of requests that were rejected due to the node level limits breached
     *          i.e. when a request for a shard could not be served due to node level limit was already reached.
     * c. lastSuccessfulRequestLimitsBreachedRejections - Total number of requests that were rejected due to the
     *          last successful request limits breached for a shard i.e. complete path failure (black-hole).
     * d. throughputDegradationLimitsBreachedRejections - Total number of requests that were rejected due to the
     *          throughput degradation in the request path for a shard i.e. partial failure.
     *
     * @opensearch.internal
     */
    public static class RejectionTracker {
        /**
         * Creates a new RejectionTracker.
         */
        public RejectionTracker() {
        }

        private final AtomicLong totalRejections = new AtomicLong();
        private final AtomicLong nodeLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong lastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong throughputDegradationLimitsBreachedRejections = new AtomicLong();

        /**
         * Returns the total rejections.
         *
         * @return the total rejections
         */
        public long getTotalRejections() {
            return totalRejections.get();
        }

        /**
         * Increments the total rejections.
         *
         * @return this instance
         */
        public long incrementTotalRejections() {
            return totalRejections.incrementAndGet();
        }

        /**
         * Returns the node limits breached rejections.
         *
         * @return the node limits breached rejections
         */
        public long getNodeLimitsBreachedRejections() {
            return nodeLimitsBreachedRejections.get();
        }

        /**
         * Increments the node limits breached rejections.
         *
         * @return this instance
         */
        public long incrementNodeLimitsBreachedRejections() {
            return nodeLimitsBreachedRejections.incrementAndGet();
        }

        /**
         * Returns the last successful request limits breached rejections.
         *
         * @return the last successful request limits breached rejections
         */
        public long getLastSuccessfulRequestLimitsBreachedRejections() {
            return lastSuccessfulRequestLimitsBreachedRejections.get();
        }

        /**
         * Increments the last successful request limits breached rejections.
         *
         * @return this instance
         */
        public long incrementLastSuccessfulRequestLimitsBreachedRejections() {
            return lastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
        }

        /**
         * Returns the throughput degradation limits breached rejections.
         *
         * @return the throughput degradation limits breached rejections
         */
        public long getThroughputDegradationLimitsBreachedRejections() {
            return throughputDegradationLimitsBreachedRejections.get();
        }

        /**
         * Increments the throughput degradation limits breached rejections.
         *
         * @return this instance
         */
        public long incrementThroughputDegradationLimitsBreachedRejections() {
            return throughputDegradationLimitsBreachedRejections.incrementAndGet();
        }
    }

    /**
     * Performance Tracker is used to track the request performance statistics for every operation, per shard. This includes:
     * a. latencyInMillis - Total indexing time take by requests that were processed successfully for a shard.
     * b. lastSuccessfulRequestTimestamp - Timestamp of last successful request for a shard.
     * c. TotalOutstandingRequests - Total requests outstanding for a shard at any given point.
     * d. ThroughputMovingAverage - Total moving average throughput value for last N requests.
     * e. ThroughputMovingQueue - Queue that holds the last N requests throughput such that there exists a sliding window
     *          which keeps moving everytime a new request comes. At any given point it tracks last N requests only.
     *          EWMA cannot be used here as it evaluate the historical average, while here it just needs the average of last N requests.
     *
     * @opensearch.internal
     */
    public static class PerformanceTracker {
        /**
         * Creates a new PerformanceTracker.
         */
        public PerformanceTracker() {
        }

        private final AtomicLong latencyInMillis = new AtomicLong();
        private volatile long lastSuccessfulRequestTimestamp = 0;
        private final AtomicLong totalOutstandingRequests = new AtomicLong();
        /**
         * Shard Window Throughput Tracker.
         * We will be using atomic long to track double values as mentioned here -
         * https://docs.oracle.com/javase/6/docs/api/java/util/concurrent/atomic/package-summary.html
         */
        private final AtomicLong throughputMovingAverage = new AtomicLong();
        private final ConcurrentLinkedQueue<Double> throughputMovingQueue = new ConcurrentLinkedQueue<>();

        /**
         * Returns the latency in milliseconds.
         *
         * @return the latency in milliseconds
         */
        public long getLatencyInMillis() {
            return latencyInMillis.get();
        }

        /**
         * Adds the latency in milliseconds.
         *
         * @param latency the latency
         * @return this instance
         */
        public long addLatencyInMillis(long latency) {
            return latencyInMillis.addAndGet(latency);
        }

        /**
         * Returns the last successful request timestamp.
         *
         * @return the last successful request timestamp
         */
        public long getLastSuccessfulRequestTimestamp() {
            return lastSuccessfulRequestTimestamp;
        }

        /**
         * Updates the last successful request timestamp.
         *
         * @param timeStamp the time stamp
         */
        public void updateLastSuccessfulRequestTimestamp(long timeStamp) {
            lastSuccessfulRequestTimestamp = timeStamp;
        }

        /**
         * Returns the total outstanding requests.
         *
         * @return the total outstanding requests
         */
        public long getTotalOutstandingRequests() {
            return totalOutstandingRequests.get();
        }

        /**
         * Increments the total outstanding requests.
         *
         * @return this instance
         */
        public long incrementTotalOutstandingRequests() {
            return totalOutstandingRequests.incrementAndGet();
        }

        /**
         * Resets the total outstanding requests.
         */
        public void resetTotalOutstandingRequests() {
            totalOutstandingRequests.set(0L);
        }

        /**
         * Returns the throughput moving average.
         *
         * @return the throughput moving average
         */
        public long getThroughputMovingAverage() {
            return throughputMovingAverage.get();
        }

        /**
         * Updates the throughput moving average.
         *
         * @param newAvg the new avg
         * @return this instance
         */
        public long updateThroughputMovingAverage(long newAvg) {
            return throughputMovingAverage.getAndSet(newAvg);
        }

        /**
         * Adds the new throughout.
         *
         * @param newThroughput the new throughput
         * @return this instance
         */
        public boolean addNewThroughout(Double newThroughput) {
            return throughputMovingQueue.offer(newThroughput);
        }

        /**
         * Returns the first throughput.
         *
         * @return the first throughput
         */
        public Double getFirstThroughput() {
            return throughputMovingQueue.poll();
        }

        /**
         * Returns the throughput moving queue size.
         *
         * @return the throughput moving queue size
         */
        public long getThroughputMovingQueueSize() {
            return throughputMovingQueue.size();
        }
    }

    /**
     * Common operation tracker is used to track values applicable across the operations for a specific shard role. This includes:
     * a. currentCombinedCoordinatingAndPrimaryBytes - Bytes of data that is inflight/processing for a shard
     *      when primary is local to coordinator node. Hence common accounting for coordinator and primary operation.
     * b. totalCombinedCoordinatingAndPrimaryBytes - Total bytes that are processed/completed successfully for a shard
     *      when primary is local to coordinator node. Hence common accounting for coordinator and primary operation.
     *
     * @opensearch.internal
     */
    public static class CommonOperationTracker {
        /**
         * Creates a new CommonOperationTracker.
         */
        public CommonOperationTracker() {
        }

        private final AtomicLong currentCombinedCoordinatingAndPrimaryBytes = new AtomicLong();
        private final AtomicLong totalCombinedCoordinatingAndPrimaryBytes = new AtomicLong();

        /**
         * Returns the current combined coordinating and primary bytes.
         *
         * @return the current combined coordinating and primary bytes
         */
        public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
            return currentCombinedCoordinatingAndPrimaryBytes.get();
        }

        /**
         * Increments the current combined coordinating and primary bytes.
         *
         * @param bytes the bytes
         * @return this instance
         */
        public long incrementCurrentCombinedCoordinatingAndPrimaryBytes(long bytes) {
            return currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        }

        /**
         * Returns the total combined coordinating and primary bytes.
         *
         * @return the total combined coordinating and primary bytes
         */
        public long getTotalCombinedCoordinatingAndPrimaryBytes() {
            return totalCombinedCoordinatingAndPrimaryBytes.get();
        }

        /**
         * Increments the total combined coordinating and primary bytes.
         *
         * @param bytes the bytes
         * @return this instance
         */
        public long incrementTotalCombinedCoordinatingAndPrimaryBytes(long bytes) {
            return totalCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        }
    }
}
