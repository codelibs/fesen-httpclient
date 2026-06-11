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
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Clear Indices Cache API over HTTP for OpenSearch/Elasticsearch, clearing
 * field data, query, and request caches for indices.
 */
public class HttpClearIndicesCacheAction extends HttpAction {

    /** The clear indices cache action definition. */
    protected final ClearIndicesCacheAction action;

    /**
     * Creates a new HTTP clear indices cache action.
     *
     * @param client the HTTP client used to send requests
     * @param action the clear indices cache action definition
     */
    public HttpClearIndicesCacheAction(final HttpClient client, final ClearIndicesCacheAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the clear indices cache request and notifies the listener with the response.
     *
     * @param request the clear indices cache request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClearIndicesCacheResponse clearIndicesCacheResponse = ClearIndicesCacheResponse.fromXContent(parser);
                listener.onResponse(clearIndicesCacheResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the HTTP request for the clear indices cache API endpoint.
     *
     * @param request the clear indices cache request
     * @return the HTTP request to execute
     */
    protected CurlRequest getCurlRequest(final ClearIndicesCacheRequest request) {
        // RestClearIndicesCacheAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_cache/clear", request.indices());
        if (request.fields() != null && request.fields().length > 0) {
            curlRequest.param("fields", String.join(",", request.fields()));
        }
        curlRequest.param("fielddata", String.valueOf(request.fieldDataCache()));
        curlRequest.param("query", String.valueOf(request.queryCache()));
        curlRequest.param("request", String.valueOf(request.requestCache()));
        return curlRequest;
    }
}
