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
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Get All Point-in-Time (PIT) API over HTTP, listing every active
 * PIT reader context across the cluster.
 *
 * <p>This API is OpenSearch-specific and supported only on OpenSearch 2.x and
 * later via the {@code GET /_search/point_in_time/_all} endpoint. Elasticsearch
 * (7.x and 8.x) has no equivalent list-all endpoint, OpenSearch 1.x predates
 * PIT, and the engine cannot be determined for {@link EngineType#UNKNOWN};
 * in all of these cases {@link #execute(GetAllPitNodesRequest, ActionListener)}
 * fails the listener with an {@link UnsupportedOperationException}.
 */
public class HttpGetAllPitsAction extends HttpAction {

    /** The get all PITs action definition. */
    protected final GetAllPitsAction action;

    /**
     * Creates a new HTTP get all PITs action.
     *
     * @param client the HTTP client used to send requests
     * @param action the get all PITs action definition
     */
    public HttpGetAllPitsAction(final HttpClient client, final GetAllPitsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get all PITs request and notifies the listener with the response.
     *
     * <p>Fails the listener with an {@link UnsupportedOperationException} on any backend
     * other than OpenSearch 2.x+, since listing all PITs is OpenSearch-specific.
     *
     * @param request the get all PITs request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final GetAllPitNodesRequest request, final ActionListener<GetAllPitNodesResponse> listener) {
        final EngineType type = client.getEngineInfo().getType();
        if (type != EngineType.OPENSEARCH2 && type != EngineType.OPENSEARCH3) {
            listener.onFailure(new UnsupportedOperationException(
                    "Listing all PITs is not supported on " + type + " over HTTP (OpenSearch 2.x+ only)"));
            return;
        }
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetAllPitNodesResponse pitResponse = fromXContent(parser);
                listener.onResponse(pitResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a get all PITs response from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed get all PITs response
     * @throws IOException if parsing the response fails
     */
    protected GetAllPitNodesResponse fromXContent(final XContentParser parser) throws IOException {
        return GetAllPitNodesResponse.fromXContent(parser);
    }

    /**
     * Builds the HTTP request for the get all PITs API endpoint.
     *
     * @param request the get all PITs request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final GetAllPitNodesRequest request) {
        // RestGetAllPitsAction: GET /_search/point_in_time/_all
        return client.getCurlRequest(GET, "/_search/point_in_time/_all");
    }
}
