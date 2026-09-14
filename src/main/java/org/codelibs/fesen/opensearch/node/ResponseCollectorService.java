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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.node;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.concurrent.ConcurrentCollections;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
/*
 * Was `implements ClusterStateListener`. The listener half -- clusterChanged and the node
 * bookkeeping it drove -- is how a node maintained these statistics. Everything here uses
 * only the ComputedNodeStats payload, which AdaptiveSelectionStats carries in _nodes/stats.
 */
public final class ResponseCollectorService {
    /**
     * Creates a new ResponseCollectorService.
     */
    public ResponseCollectorService() {
    }

    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap();


    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ComputedNodeStats implements Writeable {
        // We store timestamps with nanosecond precision, however, the
        // formula specifies milliseconds, therefore we need to convert
        // the values so the times don't unduely weight the formula
        private final double FACTOR = 1000000.0;
        private final int clientNum;

        private double cachedRank = 0;

        /**
         * The node identifier.
         */
        public final String nodeId;
        /**
         * The queue size.
         */
        public final int queueSize;
        /**
         * The response time.
         */
        public final double responseTime;
        /**
         * The service time.
         */
        public final double serviceTime;

        /**
         * Creates a new ComputedNodeStats.
         *
         * @param nodeId the node identifier
         * @param clientNum the client num
         * @param queueSize the queue size
         * @param responseTime the response time
         * @param serviceTime the service time
         */
        public ComputedNodeStats(String nodeId, int clientNum, int queueSize, double responseTime, double serviceTime) {
            this.nodeId = nodeId;
            this.clientNum = clientNum;
            this.queueSize = queueSize;
            this.responseTime = responseTime;
            this.serviceTime = serviceTime;
        }

        ComputedNodeStats(int clientNum, NodeStatistics nodeStats) {
            this(
                nodeStats.nodeId,
                clientNum,
                (int) nodeStats.queueSize.getAverage(),
                nodeStats.responseTime.getAverage(),
                nodeStats.serviceTime
            );
        }

        ComputedNodeStats(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.clientNum = in.readInt();
            this.queueSize = in.readInt();
            this.responseTime = in.readDouble();
            this.serviceTime = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeInt(this.clientNum);
            out.writeInt(this.queueSize);
            out.writeDouble(this.responseTime);
            out.writeDouble(this.serviceTime);
        }

        /**
         * Rank this copy of the data, according to the adaptive replica selection formula from the C3 paper
         * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
         */
        private double innerRank(long outstandingRequests) {
            // the concurrency compensation is defined as the number of
            // outstanding requests from the client to the node times the number
            // of clients in the system
            double concurrencyCompensation = outstandingRequests * clientNum;

            // Cubic queue adjustment factor. The paper chose 3 though we could
            // potentially make this configurable if desired.
            int queueAdjustmentFactor = 3;

            // EWMA of queue size
            double qBar = queueSize;
            double qHatS = 1 + concurrencyCompensation + qBar;

            // EWMA of response time
            double rS = responseTime / FACTOR;
            // EWMA of service time
            double muBarS = serviceTime / FACTOR;

            // The final formula
            double rank = rS - (1.0 / muBarS) + (Math.pow(qHatS, queueAdjustmentFactor) / muBarS);
            return rank;
        }

        /**
         * Ranks this instance.
         *
         * @param outstandingRequests the outstanding requests
         * @return this instance
         */
        public double rank(long outstandingRequests) {
            if (cachedRank == 0) {
                cachedRank = innerRank(outstandingRequests);
            }
            return cachedRank;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("nodes: ").append(clientNum);
            sb.append(", queue: ").append(queueSize);
            sb.append(", response time: ").append(String.format(Locale.ROOT, "%.1f", responseTime));
            sb.append(", service time: ").append(String.format(Locale.ROOT, "%.1f", serviceTime));
            sb.append(", rank: ").append(String.format(Locale.ROOT, "%.1f", rank(1)));
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     */
    private static class NodeStatistics {
        final String nodeId;
        final ExponentiallyWeightedMovingAverage queueSize;
        final ExponentiallyWeightedMovingAverage responseTime;
        double serviceTime;

        NodeStatistics(
            String nodeId,
            ExponentiallyWeightedMovingAverage queueSizeEWMA,
            ExponentiallyWeightedMovingAverage responseTimeEWMA,
            double serviceTimeEWMA
        ) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
        }
    }
}
