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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitInfo;
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
 * <p>On OpenSearch 2.x+ a delete-by-id request sends a {@code {"pit_id":[...]}}
 * body to {@code DELETE /_search/point_in_time}, and the special {@code _all}
 * identifier targets {@code DELETE /_search/point_in_time/_all} with no body. On
 * Elasticsearch 7.10+ and 8.x a delete-by-id request sends a {@code {"id":"..."}}
 * body to {@code DELETE /_pit} (the endpoint accepts a single identifier only, so
 * a request carrying multiple identifiers fails the listener with an
 * {@link UnsupportedOperationException}); Elasticsearch has no delete-all
 * endpoint, so a delete-all request likewise fails the listener with an
 * {@link UnsupportedOperationException}.
 *
 * <p>PIT is not available on OpenSearch 1.x, and the engine cannot be determined
 * for {@link EngineType#UNKNOWN}; in both cases
 * {@link #execute(DeletePitRequest, ActionListener)} fails the listener with an
 * {@link UnsupportedOperationException}.
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
     * <p>Fails the listener with an {@link UnsupportedOperationException} when the
     * backend is OpenSearch 1.x or an unknown engine, or when a delete-all request
     * targets Elasticsearch (which has no delete-all endpoint).
     *
     * @param request the delete PIT request containing the PIT identifiers to release
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final DeletePitRequest request, final ActionListener<DeletePitResponse> listener) {
        final EngineType type = client.getEngineInfo().getType();
        if (type == EngineType.OPENSEARCH1 || type == EngineType.UNKNOWN) {
            listener.onFailure(new UnsupportedOperationException(
                    "Point-In-Time is not supported on " + type + " over HTTP (requires OpenSearch 2.x+ or Elasticsearch 7.10+)"));
            return;
        }
        final boolean elasticsearch = type == EngineType.ELASTICSEARCH7 || type == EngineType.ELASTICSEARCH8;
        if (elasticsearch && isDeleteAll(request)) {
            listener.onFailure(new UnsupportedOperationException("Delete-all PITs is not supported on Elasticsearch over HTTP"));
            return;
        }
        if (elasticsearch && request.getPitIds() != null && request.getPitIds().size() > 1) {
            // The Elasticsearch _pit endpoint accepts a single id per request (an array body is
            // rejected with HTTP 400); callers must delete multiple PITs individually.
            listener.onFailure(new UnsupportedOperationException(
                    "Deleting multiple PITs in a single request is not supported on Elasticsearch over HTTP; delete them individually"));
            return;
        }
        final CurlRequest curlRequest = getCurlRequest(request);
        if (!isDeleteAll(request)) {
            curlRequest.body(elasticsearch ? buildEsBody(request) : buildBody(request));
        }
        curlRequest.execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeletePitResponse pitResponse = elasticsearch ? parseEsDelete(parser, request) : fromXContent(parser);
                listener.onResponse(pitResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses an OpenSearch delete PIT response from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed delete PIT response
     * @throws IOException if parsing the response fails
     */
    protected DeletePitResponse fromXContent(final XContentParser parser) throws IOException {
        return DeletePitResponse.fromXContent(parser);
    }

    /**
     * Parses an Elasticsearch close PIT response ({@code {"succeeded":...,"num_freed":...}}) into
     * a {@link DeletePitResponse}. Elasticsearch returns only an aggregate {@code succeeded} flag
     * and no per-identifier list, so the flag is applied to each requested identifier.
     *
     * @param parser the parser positioned at the response body
     * @param request the originating delete PIT request providing the identifiers
     * @return the parsed delete PIT response
     * @throws IOException if parsing the response fails
     */
    DeletePitResponse parseEsDelete(final XContentParser parser, final DeletePitRequest request) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        final Map<String, Object> map = parser.map();
        if (!map.containsKey("succeeded")) {
            // Not a successful close-PIT body (e.g. an error response routed through the success
            // path); surface it as a failure instead of reporting every id as failed.
            throw new IOException("Unexpected delete PIT response (missing \"succeeded\"): " + map);
        }
        final boolean succeeded = Boolean.TRUE.equals(map.get("succeeded"));
        final List<DeletePitInfo> results = new ArrayList<>();
        for (final String pitId : request.getPitIds()) {
            results.add(new DeletePitInfo(succeeded, pitId));
        }
        return new DeletePitResponse(results);
    }

    /**
     * Builds the HTTP request for the delete PIT API endpoint. The endpoint is selected by the
     * engine reported by {@link HttpClient#getEngineInfo()}: Elasticsearch uses {@code /_pit},
     * OpenSearch uses {@code /_search/point_in_time} (or its {@code /_all} variant for a
     * delete-all request).
     *
     * @param request the delete PIT request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeletePitRequest request) {
        final EngineType type = client.getEngineInfo().getType();
        if (type == EngineType.ELASTICSEARCH7 || type == EngineType.ELASTICSEARCH8) {
            // Elasticsearch Close PIT API: DELETE /_pit
            return client.getCurlRequest(DELETE, "/_pit");
        }
        // OpenSearch RestDeletePitAction
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
     * Serializes the delete PIT request body as a {@code {"pit_id":[...]}} JSON object
     * for OpenSearch.
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

    /**
     * Serializes the delete PIT request body as a {@code {"id":"..."}} JSON object for
     * Elasticsearch, which accepts a single identifier only. Callers with multiple identifiers
     * are rejected earlier in {@link #execute(DeletePitRequest, ActionListener)}.
     *
     * @param request the delete PIT request
     * @return the JSON request body
     */
    protected String buildEsBody(final DeletePitRequest request) {
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("id", request.getPitIds().get(0));
            builder.endObject();
            builder.flush();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to build a request.", e);
        }
    }
}
