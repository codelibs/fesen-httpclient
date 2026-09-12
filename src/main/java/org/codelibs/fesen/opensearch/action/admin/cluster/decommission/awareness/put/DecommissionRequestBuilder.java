/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.put;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.cluster.decommission.DecommissionAttribute;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Register decommission request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class DecommissionRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DecommissionRequest,
    DecommissionResponse,
    DecommissionRequestBuilder> {

    public DecommissionRequestBuilder(OpenSearchClient client, ActionType<DecommissionResponse> action, DecommissionRequest request) {
        super(client, action, request);
    }

    /**
     * @param decommissionAttribute decommission attribute
     * @return current object
     */
    public DecommissionRequestBuilder setDecommissionedAttribute(DecommissionAttribute decommissionAttribute) {
        request.setDecommissionAttribute(decommissionAttribute);
        return this;
    }

    public DecommissionRequestBuilder setDelayTimeOut(TimeValue delayTimeOut) {
        request.setDelayTimeout(delayTimeOut);
        return this;
    }

    public DecommissionRequestBuilder setNoDelay(boolean noDelay) {
        request.setNoDelay(noDelay);
        return this;
    }

    /**
     * Sets request id for decommission request
     *
     * @param requestID for decommission request
     * @return current object
     */
    public DecommissionRequestBuilder requestID(String requestID) {
        request.setRequestID(requestID);
        return this;
    }
}
