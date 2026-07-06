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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.AbstractBulkByScrollRequest;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;

/**
 * Handles the update-by-query API over HTTP for OpenSearch/Elasticsearch, updating every
 * document that matches a query, optionally applying a script.
 */
public class HttpUpdateByQueryAction extends HttpBulkByScrollAction {

    /** The update-by-query action definition. */
    protected final UpdateByQueryAction action;

    /**
     * Creates a new HttpUpdateByQueryAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the update-by-query action definition
     */
    public HttpUpdateByQueryAction(final HttpClient client, final UpdateByQueryAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the update-by-query request and notifies the listener with the response.
     *
     * @param request the update-by-query request
     * @param listener the listener to notify with the response or a failure
     */
    public void execute(final UpdateByQueryRequest request, final ActionListener<BulkByScrollResponse> listener) {
        getCurlRequest(request).body(toSource(request)).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                listener.onResponse(fromXContent(parser));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the update-by-query API. The query, {@code script}, and
     * {@code scroll_size} (as {@code size}) are carried in the request body produced by
     * {@link UpdateByQueryRequest#toXContent}; the remaining settings are added as parameters.
     *
     * @param request the update-by-query request
     * @return the curl request for the update-by-query endpoint
     */
    protected CurlRequest getCurlRequest(final UpdateByQueryRequest request) {
        // RestUpdateByQueryAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_update_by_query", request.indices());
        setCommonParams(curlRequest, request);
        if (!request.isAbortOnVersionConflict()) {
            curlRequest.param("conflicts", "proceed");
        }
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES) {
            curlRequest.param("max_docs", Integer.toString(request.getMaxDocs()));
        }
        if (request.getRouting() != null) {
            curlRequest.param("routing", request.getRouting());
        }
        if (request.getPipeline() != null) {
            curlRequest.param("pipeline", request.getPipeline());
        }
        return curlRequest;
    }
}
