/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.get;

import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Get decommission request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class GetDecommissionStateRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse,
    GetDecommissionStateRequestBuilder> {

    /**
     * Creates new get decommissioned attributes request builder
     */
    public GetDecommissionStateRequestBuilder(OpenSearchClient client, GetDecommissionStateAction action) {
        super(client, action, new GetDecommissionStateRequest());
    }

    /**
     * @param attributeName name of attribute
     * @return current object
     */
    public GetDecommissionStateRequestBuilder setAttributeName(String attributeName) {
        request.attributeName(attributeName);
        return this;
    }
}
