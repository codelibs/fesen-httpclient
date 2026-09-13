/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.replication;

import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for Segment Replication stats information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SegmentReplicationStatsRequest extends BroadcastRequest<SegmentReplicationStatsRequest> {
    private boolean detailed = false;       // Provides extra details in the response
    private boolean activeOnly = false;     // Only reports on active segment replication events
    private String[] shards = new String[0];

    /**
     * Constructs a request for segment replication stats information for all shards
     */
    public SegmentReplicationStatsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    /**
     * Constructs a request for segment replication stats information for all shards for the given indices
     *
     * @param indices   Comma-separated list of indices about which to gather segment replication information
     */
    public SegmentReplicationStatsRequest(String... indices) {
        super(indices, IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);
    }

    /**
     * True if detailed flag is set, false otherwise. This value if false by default.
     *
     * @return  True if detailed flag is set, false otherwise
     */
    public boolean detailed() {
        return detailed;
    }

    /**
     * Set value of the detailed flag. Detailed requests will contain extra
     * information like timing metric of each stage of segment replication event.
     *
     * @param detailed  Whether or not to set the detailed flag
     */
    public void detailed(boolean detailed) {
        this.detailed = detailed;
    }

    /**
     * True if activeOnly flag is set, false otherwise. This value is false by default.
     *
     * @return  True if activeOnly flag is set, false otherwise
     */
    public boolean activeOnly() {
        return activeOnly;
    }

    /**
     * Set value of the activeOnly flag. If true, this request will only respond with
     * on-going segment replication event information.
     *
     * @param activeOnly    Whether or not to set the activeOnly flag.
     */
    public void activeOnly(boolean activeOnly) {
        this.activeOnly = activeOnly;
    }

    /**
     * Contains list of shard id's if shards are passed, empty otherwise. Array is empty by default.
     *
     * @return  array of shard id's if shards are passed, empty otherwise
     */
    public String[] shards() {
        return shards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(activeOnly);
    }
}
