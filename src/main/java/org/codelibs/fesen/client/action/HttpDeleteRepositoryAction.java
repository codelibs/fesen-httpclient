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
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the delete snapshot repository API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpDeleteRepositoryAction extends HttpAction {

    /** The delete repository action definition. */
    protected final DeleteRepositoryAction action;

    /**
     * Creates a new HttpDeleteRepositoryAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the delete repository action definition
     */
    public HttpDeleteRepositoryAction(final HttpClient client, final DeleteRepositoryAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the delete repository request asynchronously and notifies the listener with the result.
     *
     * @param request the delete repository request
     * @param listener the listener to notify with the acknowledged response or a failure
     */
    public void execute(final DeleteRepositoryRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse deleteRepositoryResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(deleteRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the HTTP request for the delete repository request.
     *
     * @param request the delete repository request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeleteRepositoryRequest request) {
        // RestVerifyRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_snapshot/" + UrlUtils.encode(request.name()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
