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
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.usage.NodeUsage;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageAction;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.XContentParser;

public class HttpNodesUsageAction extends HttpAction {

    protected NodesUsageAction action;

    public HttpNodesUsageAction(final HttpClient client, final NodesUsageAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final NodesUsageRequest request, final ActionListener<NodesUsageResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final NodesUsageResponse nodesUsageResponse = fromXContent(parser);
                listener.onResponse(nodesUsageResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected NodesUsageResponse fromXContent(final XContentParser parser) throws IOException {
        List<NodeUsage> nodes = Collections.emptyList();
        String fieldName = null;
        ClusterName clusterName = ClusterName.DEFAULT;

        // Initialize parser - move to START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_nodes".equals(fieldName)) {
                    consumeObject(parser);
                } else if ("nodes".equals(fieldName)) {
                    nodes = parseNodes(parser);
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("cluster_name".equals(fieldName)) {
                    clusterName = new ClusterName(parser.text());
                }
            }
        }

        return new NodesUsageResponse(clusterName, nodes, Collections.emptyList());
    }

    protected List<NodeUsage> parseNodes(final XContentParser parser) throws IOException {
        final List<NodeUsage> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                list.add(parseNodeUsage(parser, fieldName));
            }
        }

        return list;
    }

    protected NodeUsage parseNodeUsage(final XContentParser parser, final String nodeId) throws IOException {
        String fieldName = null;
        long timestamp = 0;
        long sinceTime = 0;
        Map<String, Long> restUsage = Collections.emptyMap();
        Map<String, Object> aggregationUsage = Collections.emptyMap();
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                } else if ("since".equals(fieldName)) {
                    sinceTime = parser.longValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("rest_actions".equals(fieldName)) {
                    restUsage = parseRestActions(parser);
                } else if ("aggregations".equals(fieldName)) {
                    // Aggregations is Map<String, Object> with nested structure; skip for simplicity
                    consumeObject(parser);
                } else {
                    consumeObject(parser);
                }
            }
        }

        final DiscoveryNode node = new DiscoveryNode(nodeId, nodeId, new TransportAddress(TransportAddress.META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        return new NodeUsage(node, timestamp, sinceTime, restUsage, aggregationUsage);
    }

    protected Map<String, Long> parseRestActions(final XContentParser parser) throws IOException {
        final Map<String, Long> restActions = new HashMap<>();
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String key = parser.currentName();
                parser.nextToken();
                restActions.put(key, parser.longValue());
            }
        }

        return restActions;
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

    protected CurlRequest getCurlRequest(final NodesUsageRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_nodes");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }
        buf.append("/usage");
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
