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

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingNode;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRoutingState;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.RerouteExplanation;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.decider.Decision;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A command that moves a shard from a specific node to another node.<br>
 * <b>Note:</b> The shard needs to be in the state
 * {@link ShardRoutingState#STARTED} in order to be moved.
 *
 * @opensearch.internal
 */
public class MoveAllocationCommand implements AllocationCommand {

    public static final String NAME = "move";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private final String index;
    private final int shardId;
    private final String fromNode;
    private final String toNode;

    public MoveAllocationCommand(String index, int shardId, String fromNode, String toNode) {
        this.index = index;
        this.shardId = shardId;
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    /**
     * Read from a stream.
     */
    public MoveAllocationCommand(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        fromNode = in.readString();
        toNode = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(fromNode);
        out.writeString(toNode);
    }

    @Override
    public String name() {
        return NAME;
    }

    public String index() {
        return index;
    }

    public int shardId() {
        return this.shardId;
    }

    public String fromNode() {
        return this.fromNode;
    }

    public String toNode() {
        return this.toNode;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index());
        builder.field("shard", shardId());
        builder.field("from_node", fromNode());
        builder.field("to_node", toNode());
        return builder.endObject();
    }

    public static MoveAllocationCommand fromXContent(XContentParser parser) throws IOException {
        String index = null;
        int shardId = -1;
        String fromNode = null;
        String toNode = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    index = parser.text();
                } else if ("shard".equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if ("from_node".equals(currentFieldName) || "fromNode".equals(currentFieldName)) {
                    fromNode = parser.text();
                } else if ("to_node".equals(currentFieldName) || "toNode".equals(currentFieldName)) {
                    toNode = parser.text();
                } else {
                    throw new OpenSearchParseException("[{}] command does not support field [{}]", NAME, currentFieldName);
                }
            } else {
                throw new OpenSearchParseException("[{}] command does not support complex json tokens [{}]", NAME, token);
            }
        }
        if (index == null) {
            throw new OpenSearchParseException("[{}] command missing the index parameter", NAME);
        }
        if (shardId == -1) {
            throw new OpenSearchParseException("[{}] command missing the shard parameter", NAME);
        }
        if (fromNode == null) {
            throw new OpenSearchParseException("[{}] command missing the from_node parameter", NAME);
        }
        if (toNode == null) {
            throw new OpenSearchParseException("[{}] command missing the to_node parameter", NAME);
        }
        return new MoveAllocationCommand(index, shardId, fromNode, toNode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MoveAllocationCommand other = (MoveAllocationCommand) obj;
        // Override equals and hashCode for testing
        return Objects.equals(index, other.index)
            && Objects.equals(shardId, other.shardId)
            && Objects.equals(fromNode, other.fromNode)
            && Objects.equals(toNode, other.toNode);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(index, shardId, fromNode, toNode);
    }
}
