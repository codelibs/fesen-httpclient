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
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.stats.WlmStats;

public class HttpWlmStatsAction extends HttpAction {

    protected WlmStatsAction action;

    public HttpWlmStatsAction(final HttpClient client, final WlmStatsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final WlmStatsRequest request, final ActionListener<WlmStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final WlmStatsResponse wlmStatsResponse = fromXContent(parser);
                listener.onResponse(wlmStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected WlmStatsResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        ClusterName clusterName = ClusterName.DEFAULT;
        final List<WlmStats> nodes = new ArrayList<>();

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
                    // Skip _nodes section
                    consumeObject(parser);
                } else {
                    // Each node is represented as a key-value pair
                    // Skip for now as parsing WlmStats is complex
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("cluster_name".equals(fieldName)) {
                    clusterName = new ClusterName(parser.text());
                }
            }
        }

        // Return response with empty nodes list
        // Full parsing of WlmStats is complex
        return new WlmStatsResponse(clusterName, nodes, Collections.emptyList());
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

    protected CurlRequest getCurlRequest(final WlmStatsRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_wlm");

        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }

        buf.append("/stats");

        if (!request.getWorkloadGroupIds().isEmpty() && !request.getWorkloadGroupIds().contains("_all")) {
            buf.append('/').append(String.join(",", request.getWorkloadGroupIds()));
        }

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());

        if (request.isBreach() != null && request.isBreach()) {
            curlRequest.param("breach", "true");
        }

        return curlRequest;
    }
}
