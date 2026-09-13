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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.cluster.routing.allocation;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.decider.Decision;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;

import static org.codelibs.fesen.opensearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;

/**
 * This class represents the shard allocation decision and its explanation for a single node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodeAllocationResult implements ToXContentObject, Writeable, Comparable<NodeAllocationResult> {

    private static final Comparator<NodeAllocationResult> nodeResultComparator = Comparator.comparing(NodeAllocationResult::getNodeDecision)
        .thenComparingInt(NodeAllocationResult::getWeightRanking)
        .thenComparing(r -> r.getNode().getId());

    private final DiscoveryNode node;
    @Nullable
    private final ShardStoreInfo shardStoreInfo;
    private final AllocationDecision nodeDecision;
    @Nullable
    private final Decision canAllocateDecision;
    private final int weightRanking;

    public NodeAllocationResult(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardStoreInfo = in.readOptionalWriteable(ShardStoreInfo::new);
        canAllocateDecision = in.readOptionalWriteable(Decision::readFrom);
        nodeDecision = AllocationDecision.readFrom(in);
        weightRanking = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeToWithAttribute(out);
        out.writeOptionalWriteable(shardStoreInfo);
        out.writeOptionalWriteable(canAllocateDecision);
        nodeDecision.writeTo(out);
        out.writeVInt(weightRanking);
    }

    /**
     * Get the node that this decision is for.
     */
    public DiscoveryNode getNode() {
        return node;
    }

    /**
     * Get the shard store information for the node, if it exists.
     */
    @Nullable
    public ShardStoreInfo getShardStoreInfo() {
        return shardStoreInfo;
    }

    /**
     * Is the weight assigned for the node?
     */
    public boolean isWeightRanked() {
        return weightRanking > 0;
    }

    /**
     * The weight ranking for allocating a shard to the node.  Each node will have
     * a unique weight ranking that is relative to the other nodes against which the
     * deciders ran.  For example, suppose there are 3 nodes which the allocation deciders
     * decided upon: node1, node2, and node3.  If node2 had the best weight for holding the
     * shard, followed by node3, followed by node1, then node2's weight will be 1, node3's
     * weight will be 2, and node1's weight will be 1.  A value of 0 means the weight was
     * not calculated or factored into the decision.
     */
    public int getWeightRanking() {
        return weightRanking;
    }

    /**
     * Gets the {@link AllocationDecision} for allocating to this node.
     */
    public AllocationDecision getNodeDecision() {
        return nodeDecision;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            discoveryNodeToXContent(node, false, builder);
            builder.field("node_decision", nodeDecision);
            if (shardStoreInfo != null) {
                shardStoreInfo.toXContent(builder, params);
            }
            if (isWeightRanked()) {
                builder.field("weight_ranking", getWeightRanking());
            }
            if (canAllocateDecision != null && canAllocateDecision.getDecisions().isEmpty() == false) {
                builder.startArray("deciders");
                canAllocateDecision.toXContent(builder, params);
                builder.endArray();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int compareTo(NodeAllocationResult other) {
        return nodeResultComparator.compare(this, other);
    }

    /**
     * A class that captures metadata about a shard store on a node.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class ShardStoreInfo implements ToXContentFragment, Writeable {
        private final boolean inSync;
        @Nullable
        private final String allocationId;
        private final long matchingBytes;
        @Nullable
        private final Exception storeException;

        public ShardStoreInfo(StreamInput in) throws IOException {
            this.inSync = in.readBoolean();
            this.allocationId = in.readOptionalString();
            this.matchingBytes = in.readLong();
            this.storeException = in.readException();
        }

        /**
         * Gets the allocation id for the shard copy, if it exists.
         */
        @Nullable
        public String getAllocationId() {
            return allocationId;
        }

        /**
         * Returns {@code true} if the shard copy has a matching sync id with the primary shard.
         * Returns {@code false} if the shard copy does not have a matching sync id with the primary
         * shard, or this explanation pertains to the allocation of a primary shard, in which case
         * matching sync ids are irrelevant.
         */
        public boolean hasMatchingSyncId() {
            return matchingBytes == Long.MAX_VALUE;
        }

        /**
         * Gets the store exception when trying to read the store, if there was an error.  If
         * there was no error, returns {@code null}.
         */
        @Nullable
        public Exception getStoreException() {
            return storeException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(inSync);
            out.writeOptionalString(allocationId);
            out.writeLong(matchingBytes);
            out.writeException(storeException);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("store");
            {
                if (matchingBytes < 0) {
                    // dealing with a primary shard
                    if (allocationId == null && storeException == null) {
                        // there was no information we could obtain of any shard data on the node
                        builder.field("found", false);
                    } else {
                        builder.field("in_sync", inSync);
                    }
                }
                if (allocationId != null) {
                    builder.field("allocation_id", allocationId);
                }
                if (matchingBytes >= 0) {
                    if (hasMatchingSyncId()) {
                        builder.field("matching_sync_id", true);
                    } else {
                        builder.humanReadableField("matching_size_in_bytes", "matching_size", new ByteSizeValue(matchingBytes));
                    }
                }
                if (storeException != null) {
                    builder.startObject("store_exception");
                    OpenSearchException.generateThrowableXContent(builder, params, storeException);
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }
    }

}
