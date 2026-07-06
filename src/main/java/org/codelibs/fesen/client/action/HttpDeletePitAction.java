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
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Delete Point-in-Time (PIT) API over HTTP, releasing one or more
 * PIT reader contexts.
 *
 * <p>When the request targets the special {@code _all} identifier the delete-all
 * endpoint {@code DELETE /_search/point_in_time/_all} is used with no body;
 * otherwise the PIT identifiers are sent as a {@code {"pit_id":[...]}} body to
 * {@code DELETE /_search/point_in_time}.
 *
 * <p>This action targets the OpenSearch REST API only. The
 * {@code _search/point_in_time} endpoint is OpenSearch-specific; Elasticsearch
 * uses a different {@code _pit} endpoint and Elasticsearch 7 has no PIT support
 * at all. No Elasticsearch adaptation is provided.
 */
public class HttpDeletePitAction extends HttpAction {

    /** The identifier used to request deletion of all PIT contexts. */
    protected static final String ALL_PIT_IDS = "_all";

    /** The delete PIT action definition. */
    protected final DeletePitAction action;

    /**
     * Creates a new HTTP delete PIT action.
     *
     * @param client the HTTP client used to send requests
     * @param action the delete PIT action definition
     */
    public HttpDeletePitAction(final HttpClient client, final DeletePitAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the delete PIT request and notifies the listener with the response.
     *
     * @param request the delete PIT request containing the PIT identifiers to release
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final DeletePitRequest request, final ActionListener<DeletePitResponse> listener) {
        final CurlRequest curlRequest = getCurlRequest(request);
        if (!isDeleteAll(request)) {
            curlRequest.body(buildBody(request));
        }
        curlRequest.execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeletePitResponse pitResponse = fromXContent(parser);
                listener.onResponse(pitResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a delete PIT response from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed delete PIT response
     * @throws IOException if parsing the response fails
     */
    protected DeletePitResponse fromXContent(final XContentParser parser) throws IOException {
        return DeletePitResponse.fromXContent(parser);
    }

    /**
     * Builds the HTTP request for the delete PIT API endpoint. A delete-all request
     * targets the {@code /_all} endpoint; otherwise the plain endpoint is used.
     *
     * @param request the delete PIT request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeletePitRequest request) {
        // RestDeletePitAction
        if (isDeleteAll(request)) {
            return client.getCurlRequest(DELETE, "/_search/point_in_time/_all");
        }
        return client.getCurlRequest(DELETE, "/_search/point_in_time");
    }

    /**
     * Determines whether the request represents a delete-all operation, that is a
     * single PIT identifier equal to {@code _all}.
     *
     * @param request the delete PIT request
     * @return {@code true} if the request should target the delete-all endpoint
     */
    protected static boolean isDeleteAll(final DeletePitRequest request) {
        final List<String> pitIds = request.getPitIds();
        return pitIds != null && pitIds.size() == 1 && ALL_PIT_IDS.equals(pitIds.get(0));
    }

    /**
     * Serializes the delete PIT request body as a {@code {"pit_id":[...]}} JSON object.
     *
     * @param request the delete PIT request
     * @return the JSON request body
     */
    protected String buildBody(final DeletePitRequest request) {
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to build a request.", e);
        }
    }
}
