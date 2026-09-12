/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.search;

import org.codelibs.fesen.opensearch.action.search.PutSearchPipelineRequest;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST action to put a search pipeline
 *
 * @opensearch.internal
 */
public class RestPutSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_put_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Tuple<MediaType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        PutSearchPipelineRequest request = new PutSearchPipelineRequest(restRequest.param("id"), sourceTuple.v2(), sourceTuple.v1());
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().putSearchPipeline(request, new RestToXContentListener<>(channel));
    }
}
