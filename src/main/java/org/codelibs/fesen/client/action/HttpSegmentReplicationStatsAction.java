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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsAction;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsRequest;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.SegmentReplicationPerGroupStats;

/**
 * Handles the segment replication stats API over HTTP for OpenSearch/Elasticsearch.
 * Uses the CAT segment replication endpoint to retrieve replication statistics.
 */
public class HttpSegmentReplicationStatsAction extends HttpAction {

    /** The segment replication stats action definition. */
    protected SegmentReplicationStatsAction action;

    /**
     * Creates a new HTTP segment replication stats action.
     *
     * @param client the HTTP client used to send requests
     * @param action the segment replication stats action definition
     */
    public HttpSegmentReplicationStatsAction(final HttpClient client, final SegmentReplicationStatsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the segment replication stats request and notifies the listener with the response.
     *
     * @param request the segment replication stats request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final SegmentReplicationStatsRequest request, final ActionListener<SegmentReplicationStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SegmentReplicationStatsResponse segmentReplicationStatsResponse = fromXContent(parser);
                listener.onResponse(segmentReplicationStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses the CAT segment replication response. Since the CAT API response format does not
     * match the expected {@link SegmentReplicationStatsResponse} structure, the content is
     * consumed and an empty response is returned.
     *
     * @param parser the parser positioned at the response content
     * @return an empty segment replication stats response
     * @throws IOException if parsing fails
     */
    protected SegmentReplicationStatsResponse fromXContent(final XContentParser parser) throws IOException {
        // CAT API returns an array of objects, not a standard response format
        // We need to handle this differently
        final Map<String, List<SegmentReplicationPerGroupStats>> replicationStats = new HashMap<>();
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        // Initialize parser - move to first token
        XContentParser.Token token = parser.nextToken();

        // CAT API with format=json returns an array
        if (token == XContentParser.Token.START_ARRAY) {
            // Skip the array - we can't easily convert CAT response to SegmentReplicationPerGroupStats
            consumeArray(parser);
        } else if (token == XContentParser.Token.START_OBJECT) {
            // Standard response format (if exists)
            consumeObject(parser);
        }

        // Return empty response as CAT API response format doesn't match
        // SegmentReplicationStatsResponse expected structure
        return new SegmentReplicationStatsResponse(0, // totalShards
                0, // successfulShards
                0, // failedShards
                replicationStats, shardFailures);
    }

    /**
     * Consumes the current object from the parser, including all nested objects and arrays.
     *
     * @param parser the parser positioned inside the object to consume
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
     * Consumes the current array from the parser, including all nested objects and arrays.
     *
     * @param parser the parser positioned inside the array to consume
     * @throws IOException if parsing fails
     */
    protected void consumeArray(final XContentParser parser) throws IOException {
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
     * Builds the curl request for the segment replication stats API.
     *
     * @param request the segment replication stats request
     * @return the curl request for the CAT segment replication endpoint
     */
    protected CurlRequest getCurlRequest(final SegmentReplicationStatsRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_cat/segment_replication");
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());

        // Request JSON format
        curlRequest.param("format", "json");

        if (request.detailed()) {
            curlRequest.param("detailed", "true");
        }
        if (request.activeOnly()) {
            curlRequest.param("active_only", "true");
        }
        if (request.shards() != null && request.shards().length > 0) {
            curlRequest.param("shards", String.join(",", request.shards()));
        }

        return curlRequest;
    }
}
