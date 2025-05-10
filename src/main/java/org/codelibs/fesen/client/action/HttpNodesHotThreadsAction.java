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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;

public class HttpNodesHotThreadsAction extends HttpAction {

    protected NodesHotThreadsAction action;

    public HttpNodesHotThreadsAction(final HttpClient client, final NodesHotThreadsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final NodesHotThreadsRequest request, final ActionListener<NodesHotThreadsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getContentAsStream(), StandardCharsets.UTF_8))) {
                final List<NodeHotThreads> nodes = new ArrayList<>();
                DiscoveryNode node = null;
                final StringBuilder buf = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("::: ")) {
                        if (node != null) {
                            nodes.add(new NodeHotThreads(node, buf.toString().trim()));
                            buf.setLength(0);
                        }
                        node = parseDiscoveryNode(line);
                    } else {
                        buf.append(line).append('\n');
                    }
                }
                if (node != null) {
                    nodes.add(new NodeHotThreads(node, buf.toString().trim()));
                }
                listener.onResponse(new NodesHotThreadsResponse(new ClusterName(client.getEngineInfo().getClusterName()), nodes,
                        Collections.emptyList()));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected DiscoveryNode parseDiscoveryNode(final String line) {
        final List<String> list = new ArrayList<>();
        boolean isTarget = false;
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '{') {
                isTarget = true;
            } else if (line.charAt(i) == '}') {
                isTarget = false;
                list.add(buf.toString());
                buf.setLength(0);
            } else if (isTarget) {
                buf.append(line.charAt(i));
            }
        }

        if (list.size() >= 5) {
            try {
                return new DiscoveryNode(list.get(0), list.get(1), list.get(2), list.get(3), list.get(4), null, Collections.emptyMap(),
                        Collections.emptySet(), Version.V_EMPTY);
            } catch (final Exception e) {
                // ignore
            }
        }

        if (list.size() >= 4) {
            try {
                return new DiscoveryNode(list.get(0), list.get(1), list.get(2), list.get(3), "0.0.0.0", null, Collections.emptyMap(),
                        Collections.emptySet(), Version.V_EMPTY);
            } catch (final Exception e) {
                // ignore
            }
        }

        if (list.size() >= 3) {
            return new DiscoveryNode(list.get(0), list.get(1), list.get(2), "unknown", "0.0.0.0", null, Collections.emptyMap(),
                    Collections.emptySet(), Version.V_EMPTY);
        }

        if (list.size() >= 2) {
            return new DiscoveryNode(list.get(0), list.get(1), "unknown", "unknown", "0.0.0.0", null, Collections.emptyMap(),
                    Collections.emptySet(), Version.V_EMPTY);
        }

        return new DiscoveryNode("unknown", "unknown", "unknown", "unknown", "0.0.0.0", null, Collections.emptyMap(),
                Collections.emptySet(), Version.V_EMPTY);
    }

    protected CurlRequest getCurlRequest(final NodesHotThreadsRequest request) {
        // RestNodesHotThreadsAction
        final StringBuilder buf = new StringBuilder();
        buf.append("/_nodes");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }
        buf.append("/hot_threads");
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        curlRequest.param("threads", String.valueOf(request.threads()));
        curlRequest.param("ignore_idle_threads", request.ignoreIdleThreads() ? "true" : "false");
        if (request.type() != null) {
            curlRequest.param("type", request.type());
        }
        if (request.interval() != null) {
            curlRequest.param("interval", request.interval().toString());
        }
        curlRequest.param("snapshots", String.valueOf(request.snapshots()));
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
