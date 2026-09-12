/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.indices;

import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.codelibs.fesen.opensearch.action.pagination.PageParams;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest.DEFAULT_PAGE_SIZE;
import static org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest.DEFAULT_SORT_VALUE;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get ingestion state. This API supports pagination.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class RestGetIngestionStateAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/{index}/ingestion/_state")));
    }

    @Override
    public String getName() {
        return "get_ingestion_state_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetIngestionStateRequest getIngestionStateRequest = new GetIngestionStateRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );

        if (request.hasParam("shards")) {
            int[] shards = Arrays.stream(request.paramAsStringArrayOrEmptyIfAll("shards"))
                .mapToInt(Integer::parseInt) // Convert each string to int
                .toArray();
            getIngestionStateRequest.setShards(shards);
        }
        getIngestionStateRequest.timeout(request.paramAsTime("timeout", getIngestionStateRequest.timeout()));
        getIngestionStateRequest.indicesOptions(IndicesOptions.fromRequest(request, getIngestionStateRequest.indicesOptions()));

        PageParams pageParams = validateAndGetPageParams(request, DEFAULT_SORT_VALUE, DEFAULT_PAGE_SIZE);
        getIngestionStateRequest.setPageParams(pageParams);
        return channel -> client.admin().indices().getIngestionState(getIngestionStateRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }
}
