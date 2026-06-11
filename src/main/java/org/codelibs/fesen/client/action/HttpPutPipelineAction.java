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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ingest.PutPipelineAction;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the put pipeline API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpPutPipelineAction extends HttpAction {

    /** The put pipeline action definition. */
    protected final PutPipelineAction action;

    /**
     * Creates a new HttpPutPipelineAction.
     *
     * @param client the HTTP client
     * @param action the put pipeline action
     */
    public HttpPutPipelineAction(final HttpClient client, final PutPipelineAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the put pipeline request asynchronously and notifies the listener with the response or failure.
     *
     * @param request the put pipeline request
     * @param listener the listener notified with the response or failure
     */
    public void execute(final PutPipelineRequest request, final ActionListener<AcknowledgedResponse> listener) {
        String source = null;
        try {
            source = XContentHelper.convertToJson(request.getSource(), false, false, request.getMediaType());
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a reqsuest.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse putPipelineResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(putPipelineResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the CURL request for the put pipeline request.
     *
     * @param request the put pipeline request
     * @return the CURL request
     */
    protected CurlRequest getCurlRequest(final PutPipelineRequest request) {
        // RestPutPipelineAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/_ingest/pipeline/" + UrlUtils.encode(request.getId()));
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
