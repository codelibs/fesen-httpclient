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
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the update index settings API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpUpdateSettingsAction extends HttpAction {

    /** The update settings action definition. */
    protected final UpdateSettingsAction action;

    /**
     * Creates a new HTTP update settings action.
     *
     * @param client the HTTP client used to send requests
     * @param action the update settings action definition
     */
    public HttpUpdateSettingsAction(final HttpClient client, final UpdateSettingsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the update settings request and notifies the listener with the response.
     *
     * @param request the update settings request
     * @param listener the listener notified with the acknowledged response or a failure
     */
    public void execute(final UpdateSettingsRequest request, final ActionListener<AcknowledgedResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse updateSettingsResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(updateSettingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the update settings API.
     *
     * @param request the update settings request
     * @return the curl request for the settings endpoint
     */
    protected CurlRequest getCurlRequest(final UpdateSettingsRequest request) {
        // RestUpdateSettingsAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/_settings", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        curlRequest.param("preserve_existing", Boolean.toString(request.isPreserveExisting()));
        return curlRequest;
    }
}
