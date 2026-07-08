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
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the put repository API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpPutRepositoryAction extends HttpAction {

    /** The put repository action definition. */
    protected final PutRepositoryAction action;

    /**
     * Creates a new HttpPutRepositoryAction.
     *
     * @param client the HTTP client
     * @param action the put repository action
     */
    public HttpPutRepositoryAction(final HttpClient client, final PutRepositoryAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the put repository request asynchronously and notifies the listener with the response or failure.
     *
     * @param request the put repository request
     * @param listener the listener notified with the response or failure
     */
    public void execute(final PutRepositoryRequest request, final ActionListener<AcknowledgedResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse putRepositoryResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(putRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the CURL request for the put repository request.
     *
     * @param request the put repository request
     * @return the CURL request
     */
    protected CurlRequest getCurlRequest(final PutRepositoryRequest request) {
        // RestPutRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/_snapshot/" + UrlUtils.encode(request.name()));
        curlRequest.param("verify", Boolean.toString(request.verify()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
