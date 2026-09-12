/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Request builder to delete weights for weighted round-robin shard routing policy.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class ClusterDeleteWeightedRoutingRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterDeleteWeightedRoutingRequest,
    ClusterDeleteWeightedRoutingResponse,
    ClusterDeleteWeightedRoutingRequestBuilder> {

    public ClusterDeleteWeightedRoutingRequestBuilder(OpenSearchClient client, ClusterDeleteWeightedRoutingAction action) {
        super(client, action, new ClusterDeleteWeightedRoutingRequest());
    }

    public ClusterDeleteWeightedRoutingRequestBuilder setVersion(long version) {
        request.setVersion(version);
        return this;
    }

    public ClusterDeleteWeightedRoutingRequestBuilder setAwarenessAttribute(String attribute) {
        request.setAwarenessAttribute(attribute);
        return this;
    }

}
