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
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Force Merge API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpForceMergeAction extends HttpAction {

    /** The force merge action definition. */
    protected final ForceMergeAction action;

    /**
     * Creates a new HTTP force merge action.
     *
     * @param client the HTTP client used to send requests
     * @param action the force merge action definition
     */
    public HttpForceMergeAction(final HttpClient client, final ForceMergeAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the force merge request asynchronously.
     *
     * @param request the force merge request
     * @param listener the listener notified with the force merge response or a failure
     */
    public void execute(final ForceMergeRequest request, final ActionListener<ForceMergeResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ForceMergeResponse forceMergeResponse = ForceMergeResponse.fromXContent(parser);
                listener.onResponse(forceMergeResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the force merge API.
     *
     * @param request the force merge request
     * @return the curl request to send
     */
    protected CurlRequest getCurlRequest(final ForceMergeRequest request) {
        // RestForceMergeAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_forcemerge", request.indices());
        appendIndicesOptions(curlRequest, request.indicesOptions());
        curlRequest.param("max_num_segments", String.valueOf(request.maxNumSegments()))
                .param("only_expunge_deletes", String.valueOf(request.onlyExpungeDeletes()))
                .param("flush", String.valueOf(request.flush()));
        // primary_only is only recognized by OpenSearch 2.x and later; older OpenSearch and
        // Elasticsearch reject unknown parameters with HTTP 400.
        final EngineType engineType = client.getEngineInfo().getType();
        if (engineType == EngineType.OPENSEARCH2 || engineType == EngineType.OPENSEARCH3) {
            curlRequest.param("primary_only", String.valueOf(request.primaryOnly()));
        }
        return curlRequest;
    }
}
