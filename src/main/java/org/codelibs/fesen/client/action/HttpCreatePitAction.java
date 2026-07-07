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
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Create Point-in-Time (PIT) API over HTTP, opening a PIT reader
 * context over one or more indices that can be reused across search requests.
 *
 * <p>This action targets the OpenSearch REST API only. The
 * {@code /{index}/_search/point_in_time} endpoint is OpenSearch-specific;
 * Elasticsearch uses a different {@code _pit} endpoint and Elasticsearch 7 has
 * no PIT support at all. No Elasticsearch adaptation is provided.
 */
public class HttpCreatePitAction extends HttpAction {

    /** The create PIT action definition. */
    protected final CreatePitAction action;

    /**
     * Creates a new HTTP create PIT action.
     *
     * @param client the HTTP client used to send requests
     * @param action the create PIT action definition
     */
    public HttpCreatePitAction(final HttpClient client, final CreatePitAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the create PIT request and notifies the listener with the response.
     *
     * @param request the create PIT request containing the target indices and keep-alive
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final CreatePitRequest request, final ActionListener<CreatePitResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreatePitResponse pitResponse = fromXContent(parser);
                listener.onResponse(pitResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a create PIT response from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed create PIT response
     * @throws IOException if parsing the response fails
     */
    protected CreatePitResponse fromXContent(final XContentParser parser) throws IOException {
        return CreatePitResponse.fromXContent(parser);
    }

    /**
     * Builds the HTTP request for the create PIT API endpoint.
     *
     * @param request the create PIT request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final CreatePitRequest request) {
        // RestCreatePitAction: POST /{index}/_search/point_in_time
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_search/point_in_time", request.indices());
        if (request.getKeepAlive() != null) {
            curlRequest.param("keep_alive", request.getKeepAlive().getStringRep());
        }
        curlRequest.param("allow_partial_pit_creation", Boolean.toString(request.shouldAllowPartialPitCreation()));
        if (request.getPreference() != null) {
            curlRequest.param("preference", request.getPreference());
        }
        if (request.getRouting() != null) {
            curlRequest.param("routing", request.getRouting());
        }
        final IndicesOptions indicesOptions = request.indicesOptions();
        if (indicesOptions != null) {
            appendIndicesOptions(curlRequest, indicesOptions);
        }
        return curlRequest;
    }
}
