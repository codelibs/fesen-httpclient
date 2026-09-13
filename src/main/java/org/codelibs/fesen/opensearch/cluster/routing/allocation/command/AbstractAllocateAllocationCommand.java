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

package org.codelibs.fesen.opensearch.cluster.routing.allocation.command;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.RecoverySource;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingNode;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingNodes;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.routing.UnassignedInfo;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.RerouteExplanation;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.decider.Decision;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract base class for allocating an unassigned shard to a node
 *
 * @opensearch.internal
 */
public abstract class AbstractAllocateAllocationCommand implements AllocationCommand {

    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";
    private static final String NODE_FIELD = "node";

    protected static <T extends Builder<?>> ObjectParser<T, Void> createAllocateParser(String command) {
        ObjectParser<T, Void> parser = new ObjectParser<>(command);
        parser.declareString(Builder::setIndex, new ParseField(INDEX_FIELD));
        parser.declareInt(Builder::setShard, new ParseField(SHARD_FIELD));
        parser.declareString(Builder::setNode, new ParseField(NODE_FIELD));
        return parser;
    }

    /**
     * Works around ObjectParser not supporting constructor arguments.
     */
    protected abstract static class Builder<T extends AbstractAllocateAllocationCommand> {
        protected String index;
        protected int shard = -1;
        protected String node;

        public void setIndex(String index) {
            this.index = index;
        }

        public void setShard(int shard) {
            this.shard = shard;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public abstract Builder<T> parse(XContentParser parser) throws IOException;

        public abstract T build();

        protected void validate() {
            if (index == null) {
                throw new IllegalArgumentException("Argument [index] must be defined");
            }
            if (shard < 0) {
                throw new IllegalArgumentException("Argument [shard] must be defined and non-negative");
            }
            if (node == null) {
                throw new IllegalArgumentException("Argument [node] must be defined");
            }
        }
    }

    protected final String index;
    protected final int shardId;
    protected final String node;

    protected AbstractAllocateAllocationCommand(String index, int shardId, String node) {
        this.index = index;
        this.shardId = shardId;
        this.node = node;
    }

    /**
     * Read from a stream.
     */
    protected AbstractAllocateAllocationCommand(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        node = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(node);
    }

    /**
     * Get the index name
     *
     * @return name of the index
     */
    public String index() {
        return this.index;
    }

    /**
     * Get the shard id
     *
     * @return id of the shard
     */
    public int shardId() {
        return this.shardId;
    }

    /**
     * Get the id of the node
     *
     * @return id of the node
     */
    public String node() {
        return this.node;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD, index());
        builder.field(SHARD_FIELD, shardId());
        builder.field(NODE_FIELD, node());
        extraXContent(builder);
        return builder.endObject();
    }

    protected void extraXContent(XContentBuilder builder) throws IOException {}

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractAllocateAllocationCommand other = (AbstractAllocateAllocationCommand) obj;
        // Override equals and hashCode for testing
        return Objects.equals(index, other.index) && Objects.equals(shardId, other.shardId) && Objects.equals(node, other.node);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(index, shardId, node);
    }
}
