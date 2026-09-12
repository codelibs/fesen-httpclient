/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.indices;

import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to resume pull-based ingestion.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class RestResumeIngestionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/ingestion/_resume")));
    }

    @Override
    public String getName() {
        return "resume_ingestion_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        ResumeIngestionRequest resumeIngestionRequest;

        if (request.hasContent()) {
            resumeIngestionRequest = ResumeIngestionRequest.fromXContent(indices, request.contentParser());
        } else {
            resumeIngestionRequest = new ResumeIngestionRequest(indices, new ResumeIngestionRequest.ResetSettings[0]);
        }
        resumeIngestionRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", resumeIngestionRequest.clusterManagerNodeTimeout())
        );
        resumeIngestionRequest.timeout(request.paramAsTime("timeout", resumeIngestionRequest.timeout()));
        resumeIngestionRequest.indicesOptions(IndicesOptions.fromRequest(request, resumeIngestionRequest.indicesOptions()));

        return channel -> client.admin().indices().resumeIngestion(resumeIngestionRequest, new RestToXContentListener<>(channel));
    }

}
