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
import org.opensearch.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionAction;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the pause ingestion API over HTTP for OpenSearch.
 */
public class HttpPauseIngestionAction extends HttpAction {

    /** The pause ingestion action definition. */
    protected final PauseIngestionAction action;

    /**
     * Creates a new HttpPauseIngestionAction.
     *
     * @param client the HTTP client
     * @param action the pause ingestion action
     */
    public HttpPauseIngestionAction(final HttpClient client, final PauseIngestionAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the pause ingestion request asynchronously and notifies the listener with the response or failure.
     *
     * @param request the pause ingestion request
     * @param listener the listener notified with the response or failure
     */
    public void execute(final PauseIngestionRequest request, final ActionListener<PauseIngestionResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final PauseIngestionResponse pauseIngestionResponse = fromXContent(parser);
                listener.onResponse(pauseIngestionResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a pause ingestion response from the response content.
     *
     * @param parser the content parser
     * @return the pause ingestion response
     * @throws IOException if parsing fails
     */
    protected PauseIngestionResponse fromXContent(final XContentParser parser) throws IOException {
        boolean acknowledged = false;
        boolean shardsAcknowledged = false;

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String field = parser.currentName();
                parser.nextToken();
                if ("acknowledged".equals(field)) {
                    acknowledged = parser.booleanValue();
                } else if ("shards_acknowledged".equals(field)) {
                    shardsAcknowledged = parser.booleanValue();
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    consumeObject(parser);
                }
            }
        }

        return new PauseIngestionResponse(acknowledged, shardsAcknowledged, new IngestionStateShardFailure[0], "");
    }

    /**
     * Consumes and discards the current object, including any nested objects and arrays.
     *
     * @param parser the content parser
     * @throws IOException if parsing fails
     */
    protected void consumeObject(final XContentParser parser) throws IOException {
        XContentParser.Token token;
        int depth = 1;
        while (depth > 0) {
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                depth++;
            } else if (token == XContentParser.Token.END_OBJECT || token == XContentParser.Token.END_ARRAY) {
                depth--;
            }
        }
    }

    /**
     * Builds the CURL request for the pause ingestion request.
     *
     * @param request the pause ingestion request
     * @return the CURL request
     */
    protected CurlRequest getCurlRequest(final PauseIngestionRequest request) {
        // RestPauseIngestionAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/ingestion/_pause", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.clusterManagerNodeTimeout() != null) {
            curlRequest.param("cluster_manager_timeout", request.clusterManagerNodeTimeout().toString());
        }
        return curlRequest;
    }
}
