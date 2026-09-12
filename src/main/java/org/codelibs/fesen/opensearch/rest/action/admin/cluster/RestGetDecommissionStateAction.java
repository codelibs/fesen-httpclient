/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.cluster;

import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.Requests;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.GET;

/**
 * Returns decommissioned attribute information
 *
 * @opensearch.api
 */
public class RestGetDecommissionStateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/decommission/awareness/{awareness_attribute_name}/_status"));
    }

    @Override
    public String getName() {
        return "get_decommission_state_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetDecommissionStateRequest getDecommissionStateRequest = Requests.getDecommissionStateRequest();
        String attributeName = request.param("awareness_attribute_name");
        getDecommissionStateRequest.attributeName(attributeName);
        return channel -> client.admin().cluster().getDecommissionState(getDecommissionStateRequest, new RestToXContentListener<>(channel));
    }
}
