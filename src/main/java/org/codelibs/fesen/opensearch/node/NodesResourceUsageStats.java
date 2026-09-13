/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.node;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * This class represents resource usage stats such as CPU, Memory and IO resource usage of each node along with the
 * timestamp of the stats recorded.
 */
public class NodesResourceUsageStats implements Writeable, ToXContentFragment {

    // Map of node id to resource usage stats of the corresponding node.
    private final Map<String, NodeResourceUsageStats> nodeIdToResourceUsageStatsMap;

    public NodesResourceUsageStats(StreamInput in) throws IOException {
        this.nodeIdToResourceUsageStatsMap = in.readMap(StreamInput::readString, NodeResourceUsageStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.nodeIdToResourceUsageStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resource_usage_stats");
        for (String nodeId : nodeIdToResourceUsageStatsMap.keySet()) {
            NodeResourceUsageStats resourceUsageStats = nodeIdToResourceUsageStatsMap.get(nodeId);
            resourceUsageStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
