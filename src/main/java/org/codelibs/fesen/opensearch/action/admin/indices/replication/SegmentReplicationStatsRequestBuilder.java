/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.replication;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Segment Replication stats information request builder.
 *
  * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SegmentReplicationStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    SegmentReplicationStatsRequest,
    SegmentReplicationStatsResponse,
    SegmentReplicationStatsRequestBuilder> {

    public SegmentReplicationStatsRequestBuilder(OpenSearchClient client, SegmentReplicationStatsAction action) {
        super(client, action, new SegmentReplicationStatsRequest());
    }

}
