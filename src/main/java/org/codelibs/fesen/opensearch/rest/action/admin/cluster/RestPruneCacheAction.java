/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.cluster;

import org.codelibs.fesen.opensearch.action.admin.cluster.filecache.PruneFileCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.filecache.PruneFileCacheRequest;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.RestRequest.Method;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * REST action to manually trigger FileCache prune operation across multiple nodes.
 * Supports node targeting for efficient cache management.
 *
 * @opensearch.api
 */
public class RestPruneCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(Method.POST, "/_filecache/prune"));
    }

    @Override
    public String getName() {
        return "prune_filecache_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodeIds = parseNodeTargeting(request);

        PruneFileCacheRequest pruneFileCacheRequest = new PruneFileCacheRequest(nodeIds);
        pruneFileCacheRequest.timeout(request.paramAsTime("timeout", pruneFileCacheRequest.timeout()));

        return channel -> client.execute(PruneFileCacheAction.INSTANCE, pruneFileCacheRequest, new RestToXContentListener<>(channel));
    }

    private String[] parseNodeTargeting(RestRequest request) {
        String nodes = request.param("nodes");
        String node = request.param("node");

        if (nodes != null && !nodes.trim().isEmpty()) {
            String[] parsed = Strings.splitStringByCommaToArray(nodes);
            return Arrays.stream(parsed).filter(s -> s != null && !s.trim().isEmpty()).map(String::trim).toArray(String[]::new);
        } else if (node != null && !node.trim().isEmpty()) {
            return new String[] { node.trim() };
        }

        return null;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
