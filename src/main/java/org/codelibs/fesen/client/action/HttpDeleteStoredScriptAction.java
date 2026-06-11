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
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the delete stored script API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpDeleteStoredScriptAction extends HttpAction {

    /** The delete stored script action definition. */
    protected final DeleteStoredScriptAction action;

    /**
     * Creates a new HttpDeleteStoredScriptAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the delete stored script action definition
     */
    public HttpDeleteStoredScriptAction(final HttpClient client, final DeleteStoredScriptAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the delete stored script request asynchronously and notifies the listener with the result.
     *
     * @param request the delete stored script request
     * @param listener the listener to notify with the acknowledged response or a failure
     */
    public void execute(final DeleteStoredScriptRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse deleteStoredScriptResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(deleteStoredScriptResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the HTTP request for the delete stored script request.
     *
     * @param request the delete stored script request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeleteStoredScriptRequest request) {
        // RestDeleteStoredScriptAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_scripts/" + UrlUtils.encode(request.id()));
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
