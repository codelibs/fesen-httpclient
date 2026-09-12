/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for RemoteStoreStatsRequest
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    RemoteStoreStatsRequest,
    RemoteStoreStatsResponse,
    RemoteStoreStatsRequestBuilder> {

    public RemoteStoreStatsRequestBuilder(OpenSearchClient client, RemoteStoreStatsAction action) {
        super(client, action, new RemoteStoreStatsRequest());
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreStatsRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setShards(String... shards) {
        request.shards(shards);
        return this;
    }

    /**
     * Sets local shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setLocal(boolean local) {
        request.local(local);
        return this;
    }
}
