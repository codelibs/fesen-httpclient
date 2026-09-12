/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.cluster;

import org.codelibs.fesen.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.blockcache.PruneBlockCacheRequest;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * REST handler for {@code POST /_blockcache/prune}.
 *
 * <p>Clears all registered block caches on warm nodes. An optional {@code nodes}
 * parameter restricts the operation to specific node IDs.
 *
 * @opensearch.api
 */
public class RestPruneBlockCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(RestRequest.Method.POST, "/_blockcache/prune"));
    }

    @Override
    public String getName() {
        return "prune_blockcache_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodeIds = parseNodeIds(request);
        PruneBlockCacheRequest pruneRequest = nodeIds != null ? new PruneBlockCacheRequest(nodeIds) : new PruneBlockCacheRequest();
        pruneRequest.timeout(request.paramAsTime("timeout", pruneRequest.timeout()));

        return channel -> client.execute(PruneBlockCacheAction.INSTANCE, pruneRequest, new RestToXContentListener<>(channel));
    }

    private String[] parseNodeIds(RestRequest request) {
        String nodes = request.param("nodes");
        if (nodes != null && nodes.isBlank() == false) {
            return Strings.splitStringByCommaToArray(nodes);
        }
        return null;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
