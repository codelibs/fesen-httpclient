/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.ingest.GetPipelineAction;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Get Pipeline API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpGetPipelineAction extends HttpAction {

    /** The get pipeline action definition. */
    protected final GetPipelineAction action;

    /**
     * Creates a new HTTP get pipeline action.
     *
     * @param client the HTTP client used to send requests
     * @param action the get pipeline action definition
     */
    public HttpGetPipelineAction(final HttpClient client, final GetPipelineAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get pipeline request asynchronously.
     *
     * @param request the get pipeline request
     * @param listener the listener notified with the get pipeline response or a failure
     */
    public void execute(final GetPipelineRequest request, final ActionListener<GetPipelineResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetPipelineResponse getPipelineResponse = GetPipelineResponse.fromXContent(parser);
                listener.onResponse(getPipelineResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the get pipeline API.
     *
     * @param request the get pipeline request
     * @return the curl request to send
     */
    protected CurlRequest getCurlRequest(final GetPipelineRequest request) {
        // RestGetPipelineAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_ingest/pipeline/" + UrlUtils.joinAndEncode(",", request.getIds()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
