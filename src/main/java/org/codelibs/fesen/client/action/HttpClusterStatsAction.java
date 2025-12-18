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
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.cluster.stats.ClusterStatsAction;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpClusterStatsAction extends HttpAction {

    protected ClusterStatsAction action;

    public HttpClusterStatsAction(final HttpClient client, final ClusterStatsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterStatsRequest request, final ActionListener<ClusterStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterStatsResponse clusterStatsResponse = fromXContent(parser);
                listener.onResponse(clusterStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected ClusterStatsResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        String clusterUUID = null;
        long timestamp = 0;
        ClusterHealthStatus status = null;
        ClusterName clusterName = ClusterName.DEFAULT;

        // Initialize parser - move to START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        // Move to first field or END_OBJECT
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_nodes".equals(fieldName)) {
                    parseNodeResults(parser);
                } else if ("nodes".equals(fieldName)) {
                    // Skip nodes section - it's complex and not easily parseable
                    consumeObject(parser);
                } else if ("indices".equals(fieldName)) {
                    // Skip indices section - it's complex and not easily parseable
                    consumeObject(parser);
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("cluster_uuid".equals(fieldName)) {
                    clusterUUID = parser.text();
                } else if ("cluster_name".equals(fieldName)) {
                    clusterName = new ClusterName(parser.text());
                } else if ("status".equals(fieldName)) {
                    status = ClusterHealthStatus.fromString(parser.text());
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                }
            }
        }

        // Build ClusterStatsResponse with empty nodes list
        // nodesStats and indicesStats will be null, but basic fields are populated
        final ClusterState clusterState = ClusterState.builder(clusterName).build();
        final ClusterStatsResponse response = new ClusterStatsResponse(timestamp, clusterUUID, clusterName, Collections.emptyList(),
                Collections.emptyList(), clusterState, Set.of(Metric.values()), Set.of(IndexMetric.values()));

        return response;
    }

    protected void parseNodeResults(final XContentParser parser) throws IOException {
        // Skip _nodes section
        consumeObject(parser);
    }

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

    protected CurlRequest getCurlRequest(final ClusterStatsRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_cluster/stats");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append("/nodes/").append(String.join(",", request.nodesIds()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
