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
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequest;

/**
 * Handles the reindex API over HTTP for OpenSearch/Elasticsearch, copying documents
 * from a source index (or a remote cluster) into a destination index.
 */
public class HttpReindexAction extends HttpBulkByScrollAction {

    /** The reindex action definition. */
    protected final ReindexAction action;

    /**
     * Creates a new HttpReindexAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the reindex action definition
     */
    public HttpReindexAction(final HttpClient client, final ReindexAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the reindex request and notifies the listener with the response.
     *
     * @param request the reindex request
     * @param listener the listener to notify with the response or a failure
     */
    public void execute(final ReindexRequest request, final ActionListener<BulkByScrollResponse> listener) {
        getCurlRequest(request).body(toSource(request)).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                listener.onResponse(fromXContent(parser));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the reindex API. The source, destination, {@code max_docs},
     * {@code conflicts}, and {@code script} settings are carried in the request body produced
     * by {@link ReindexRequest#toXContent}; only the operational parameters are added here.
     *
     * @param request the reindex request
     * @return the curl request for the reindex endpoint
     */
    protected CurlRequest getCurlRequest(final ReindexRequest request) {
        // RestReindexAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_reindex");
        setCommonParams(curlRequest, request);
        return curlRequest;
    }
}
