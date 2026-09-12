/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.tiering;

import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.opensearch.core.common.Strings.splitStringByCommaToArray;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest Tiering API action to move indices to warm tier
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestWarmTieringAction extends BaseRestHandler {

    private static final String TARGET_TIER = "warm";

    @Override
    public List<RestHandler.Route> routes() {
        return singletonList(new RestHandler.Route(POST, "/{index}/_tier/" + TARGET_TIER));
    }

    @Override
    public String getName() {
        return "warm_tiering_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final TieringIndexRequest tieringIndexRequest = new TieringIndexRequest(
            TARGET_TIER,
            splitStringByCommaToArray(request.param("index"))
        );
        tieringIndexRequest.timeout(request.paramAsTime("timeout", tieringIndexRequest.timeout()));
        tieringIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", tieringIndexRequest.clusterManagerNodeTimeout())
        );
        tieringIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, tieringIndexRequest.indicesOptions()));
        tieringIndexRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", tieringIndexRequest.waitForCompletion()));
        return channel -> client.admin()
            .cluster()
            .execute(HotToWarmTieringAction.INSTANCE, tieringIndexRequest, new RestToXContentListener<>(channel));
    }
}
