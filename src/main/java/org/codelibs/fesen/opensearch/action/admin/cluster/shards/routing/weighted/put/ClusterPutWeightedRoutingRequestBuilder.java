/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRouting;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Request builder to update weights for weighted round-robin shard routing policy.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class ClusterPutWeightedRoutingRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse,
    ClusterPutWeightedRoutingRequestBuilder> {
    public ClusterPutWeightedRoutingRequestBuilder(OpenSearchClient client, ClusterAddWeightedRoutingAction action) {
        super(client, action, new ClusterPutWeightedRoutingRequest());
    }

    public ClusterPutWeightedRoutingRequestBuilder setWeightedRouting(WeightedRouting weightedRouting) {
        request.setWeightedRouting(weightedRouting);
        return this;
    }

    public ClusterPutWeightedRoutingRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }
}
