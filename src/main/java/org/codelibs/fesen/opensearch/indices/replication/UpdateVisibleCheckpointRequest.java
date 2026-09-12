/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.replication;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.codelibs.fesen.opensearch.indices.replication.common.SegmentReplicationTransportRequest;

import java.io.IOException;

/**
 * Request object for updating the replica's checkpoint on primary for tracking.
 *
 * @opensearch.internal
 */
public class UpdateVisibleCheckpointRequest extends SegmentReplicationTransportRequest {

    private final ReplicationCheckpoint checkpoint;
    private final ShardId primaryShardId;

    public UpdateVisibleCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        checkpoint = new ReplicationCheckpoint(in);
        primaryShardId = new ShardId(in);
    }

    public UpdateVisibleCheckpointRequest(
        long replicationId,
        String targetAllocationId,
        ShardId primaryShardId,
        DiscoveryNode targetNode,
        ReplicationCheckpoint checkpoint
    ) {
        super(replicationId, targetAllocationId, targetNode);
        this.checkpoint = checkpoint;
        this.primaryShardId = primaryShardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        checkpoint.writeTo(out);
        primaryShardId.writeTo(out);
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public ShardId getPrimaryShardId() {
        return primaryShardId;
    }
}
