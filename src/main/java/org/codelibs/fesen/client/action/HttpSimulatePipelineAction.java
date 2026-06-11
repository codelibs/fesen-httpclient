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
import org.opensearch.action.ingest.SimulatePipelineAction;
import org.opensearch.action.ingest.SimulatePipelineRequest;
import org.opensearch.action.ingest.SimulatePipelineResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the simulate pipeline API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpSimulatePipelineAction extends HttpAction {

    /** The simulate pipeline action definition. */
    protected final SimulatePipelineAction action;

    /**
     * Creates a new HTTP simulate pipeline action.
     *
     * @param client the HTTP client used to send requests
     * @param action the simulate pipeline action definition
     */
    public HttpSimulatePipelineAction(final HttpClient client, final SimulatePipelineAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the simulate pipeline request and notifies the listener with the response.
     *
     * @param request the simulate pipeline request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final SimulatePipelineRequest request, final ActionListener<SimulatePipelineResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SimulatePipelineResponse cancelTasksResponse = SimulatePipelineResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the simulate pipeline API.
     *
     * @param request the simulate pipeline request
     * @return the curl request for the simulate pipeline endpoint
     */
    protected CurlRequest getCurlRequest(final SimulatePipelineRequest request) {
        // RestSimulatePipelineAction
        final String path = request.getId() != null ? "/_ingest/pipeline/" + UrlUtils.encode(request.getId()) + "/_simulate"
                : "/_ingest/pipeline/_simulate";
        final CurlRequest curlRequest = client.getCurlRequest(POST, path);
        curlRequest.param("verbose", String.valueOf(request.isVerbose()));
        return curlRequest;
    }
}
