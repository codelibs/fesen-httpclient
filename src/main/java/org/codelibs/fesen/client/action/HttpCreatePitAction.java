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
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Create Point-in-Time (PIT) API over HTTP, opening a PIT reader
 * context over one or more indices that can be reused across search requests.
 *
 * <p>PIT is supported on OpenSearch 2.x and later via the
 * {@code POST /{index}/_search/point_in_time} endpoint, and on Elasticsearch
 * 7.10+ and 8.x via the {@code POST /{index}/_pit} endpoint. The two backends
 * differ in both endpoint and query parameters (OpenSearch accepts
 * {@code allow_partial_pit_creation}, which the Elasticsearch {@code _pit}
 * endpoint rejects), and their response bodies differ, so the engine reported by
 * {@link HttpClient#getEngineInfo()} selects the request and response handling.
 *
 * <p>PIT is not available on OpenSearch 1.x, and the engine cannot be
 * determined for {@link EngineType#UNKNOWN}; in both cases
 * {@link #execute(CreatePitRequest, ActionListener)} fails the listener with an
 * {@link UnsupportedOperationException}.
 */
public class HttpCreatePitAction extends HttpAction {

    /** The create PIT action definition. */
    protected final CreatePitAction action;

    /**
     * Creates a new HTTP create PIT action.
     *
     * @param client the HTTP client used to send requests
     * @param action the create PIT action definition
     */
    public HttpCreatePitAction(final HttpClient client, final CreatePitAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the create PIT request and notifies the listener with the response.
     *
     * <p>Fails the listener with an {@link UnsupportedOperationException} when the
     * backend is OpenSearch 1.x or an unknown engine.
     *
     * @param request the create PIT request containing the target indices and keep-alive
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final CreatePitRequest request, final ActionListener<CreatePitResponse> listener) {
        final EngineType type = client.getEngineInfo().getType();
        if (type == EngineType.OPENSEARCH1 || type == EngineType.UNKNOWN) {
            listener.onFailure(new UnsupportedOperationException(
                    "Point-In-Time is not supported on " + type + " over HTTP (requires OpenSearch 2.x+ or Elasticsearch 7.10+)"));
            return;
        }
        final boolean elasticsearch = type == EngineType.ELASTICSEARCH7 || type == EngineType.ELASTICSEARCH8;
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreatePitResponse pitResponse = elasticsearch ? parseEsCreate(parser) : fromXContent(parser);
                listener.onResponse(pitResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses an OpenSearch create PIT response from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed create PIT response
     * @throws IOException if parsing the response fails
     */
    protected CreatePitResponse fromXContent(final XContentParser parser) throws IOException {
        return CreatePitResponse.fromXContent(parser);
    }

    /**
     * Parses an Elasticsearch open PIT response ({@code {"id":...,"_shards":{...}}}) into a
     * {@link CreatePitResponse}. Elasticsearch does not report a creation time, so the current
     * system time is used, and no shard failures are reconstructed.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed create PIT response
     * @throws IOException if parsing the response fails
     */
    CreatePitResponse parseEsCreate(final XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        final Map<String, Object> map = parser.map();
        final String id = (String) map.get("id");
        if (id == null) {
            // Not a successful open-PIT body (e.g. an error response routed through the success
            // path); surface it as a failure instead of returning a response with a null id.
            throw new IOException("Unexpected create PIT response (missing \"id\"): " + map);
        }
        int total = 0;
        int successful = 0;
        int skipped = 0;
        int failed = 0;
        final Object shardsObj = map.get("_shards");
        if (shardsObj instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> shards = (Map<String, Object>) shardsObj;
            total = toInt(shards.get("total"));
            successful = toInt(shards.get("successful"));
            skipped = toInt(shards.get("skipped"));
            failed = toInt(shards.get("failed"));
        }
        return new CreatePitResponse(id, System.currentTimeMillis(), total, successful, skipped, failed, ShardSearchFailure.EMPTY_ARRAY);
    }

    private static int toInt(final Object value) {
        return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    /**
     * Builds the HTTP request for the create PIT API endpoint. The endpoint and query
     * parameters differ between OpenSearch and Elasticsearch, selected by the engine
     * reported by {@link HttpClient#getEngineInfo()}.
     *
     * @param request the create PIT request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final CreatePitRequest request) {
        final EngineType type = client.getEngineInfo().getType();
        if (type == EngineType.ELASTICSEARCH7 || type == EngineType.ELASTICSEARCH8) {
            // Elasticsearch Open PIT API: POST /{index}/_pit
            // Note: the Elasticsearch _pit endpoint does not accept allow_partial_pit_creation /
            // allow_partial_search_results (unrecognized parameter -> HTTP 400), so it is not sent.
            final CurlRequest curlRequest = client.getCurlRequest(POST, "/_pit", request.indices());
            if (request.getKeepAlive() != null) {
                curlRequest.param("keep_alive", request.getKeepAlive().getStringRep());
            }
            if (request.getPreference() != null) {
                curlRequest.param("preference", request.getPreference());
            }
            if (request.getRouting() != null) {
                curlRequest.param("routing", request.getRouting());
            }
            final IndicesOptions esIndicesOptions = request.indicesOptions();
            if (esIndicesOptions != null) {
                appendIndicesOptions(curlRequest, esIndicesOptions);
            }
            return curlRequest;
        }
        // OpenSearch RestCreatePitAction: POST /{index}/_search/point_in_time
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_search/point_in_time", request.indices());
        if (request.getKeepAlive() != null) {
            curlRequest.param("keep_alive", request.getKeepAlive().getStringRep());
        }
        curlRequest.param("allow_partial_pit_creation", Boolean.toString(request.shouldAllowPartialPitCreation()));
        if (request.getPreference() != null) {
            curlRequest.param("preference", request.getPreference());
        }
        if (request.getRouting() != null) {
            curlRequest.param("routing", request.getRouting());
        }
        final IndicesOptions indicesOptions = request.indicesOptions();
        if (indicesOptions != null) {
            appendIndicesOptions(curlRequest, indicesOptions);
        }
        return curlRequest;
    }
}
