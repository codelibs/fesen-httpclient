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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoAction;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.XContentParser;

public class HttpNodesInfoAction extends HttpAction {

    protected NodesInfoAction action;

    public HttpNodesInfoAction(final HttpClient client, final NodesInfoAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final NodesInfoResponse nodesInfoResponse = fromXContent(parser);
                listener.onResponse(nodesInfoResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected NodesInfoResponse fromXContent(final XContentParser parser) throws IOException {
        List<NodeInfo> nodes = Collections.emptyList();
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

        return new NodesInfoResponse(clusterName, nodes, Collections.emptyList());
    }

    protected List<NodeInfo> parseNodes(final XContentParser parser) throws IOException {
        final List<NodeInfo> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                list.add(parseNodeInfo(parser, fieldName));
            }
        }

        return list;
    }

    protected NodeInfo parseNodeInfo(final XContentParser parser, final String nodeId) throws IOException {
        String fieldName = null;
        String nodeName = nodeId;
        String version = Version.CURRENT.toString();
        String buildHash = "";
        String buildType = "unknown";
        final Set<DiscoveryNodeRole> roles = new HashSet<>();
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("name".equals(fieldName)) {
                    nodeName = parser.text();
                } else if ("version".equals(fieldName)) {
                    version = parser.text();
                } else if ("build_hash".equals(fieldName)) {
                    buildHash = parser.text();
                } else if ("build_type".equals(fieldName)) {
                    buildType = parser.text();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("roles".equals(fieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        final String roleName = parser.text();
                        DiscoveryNodeRole.BUILT_IN_ROLES.stream().filter(r -> r.roleName().equals(roleName)).findFirst()
                                .ifPresent(roles::add);
                    }
                } else {
                    consumeArray(parser);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            }
        }

        final DiscoveryNode discoveryNode = new DiscoveryNode(nodeName, nodeId, new TransportAddress(TransportAddress.META_ADDRESS, 0),
                Collections.emptyMap(), roles, Version.CURRENT);

        return NodeInfo.builder(Version.CURRENT, Build.CURRENT, discoveryNode).build();
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

    protected CurlRequest getCurlRequest(final NodesInfoRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_nodes");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }
        final Set<String> metrics = request.requestedMetrics();
        if (metrics != null && !metrics.isEmpty()) {
            buf.append('/').append(String.join(",", metrics));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
