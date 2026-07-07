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
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the get settings API over HTTP for OpenSearch/Elasticsearch,
 * retrieving settings of one or more indices.
 */
public class HttpGetSettingsAction extends HttpAction {

    /** The get settings action. */
    protected final GetSettingsAction action;

    /**
     * Creates a new HttpGetSettingsAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the get settings action
     */
    public HttpGetSettingsAction(final HttpClient client, final GetSettingsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get settings request and notifies the listener with the response.
     *
     * @param request the get settings request
     * @param listener the listener to notify with the response or a failure
     */
    public void execute(final GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetSettingsResponse getSettingsResponse = GetSettingsResponse.fromXContent(parser);
                listener.onResponse(getSettingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the get settings API.
     *
     * @param request the get settings request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final GetSettingsRequest request) {
        // RestGetSettingsAction
        final CurlRequest curlRequest =
                client.getCurlRequest(GET, "/_settings/" + UrlUtils.joinAndEncode(",", request.names()), request.indices());
        curlRequest.param("human", Boolean.toString(request.humanReadable()));
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        appendIndicesOptions(curlRequest, request.indicesOptions());
        return curlRequest;
    }
}
