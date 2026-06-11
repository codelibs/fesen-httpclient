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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the cluster state API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpClusterStateAction extends HttpAction {

    /** The cluster state action. */
    protected final ClusterStateAction action;

    /**
     * Creates a new HTTP cluster state action.
     *
     * @param client the HTTP client
     * @param action the cluster state action
     */
    public HttpClusterStateAction(final HttpClient client, final ClusterStateAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the cluster state request asynchronously.
     *
     * @param request the cluster state request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterStateResponse clusterStateResponse = fromXContent(parser);
                listener.onResponse(clusterStateResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a cluster state response from the given parser. Only the cluster name and the
     * wait-for-timed-out flag are extracted; the cluster state itself is returned empty because
     * it cannot be fully reconstructed from JSON.
     *
     * @param parser the parser positioned at the response body
     * @return the cluster state response
     * @throws IOException if parsing fails
     */
    protected ClusterStateResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        ClusterName clusterName = ClusterName.DEFAULT;
        boolean waitForTimedOut = false;

        final XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        XContentParser.Token currentToken;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (currentToken == XContentParser.Token.VALUE_STRING) {
                if ("cluster_name".equals(fieldName)) {
                    clusterName = new ClusterName(parser.text());
                }
            } else if (currentToken == XContentParser.Token.VALUE_BOOLEAN) {
                if ("wait_for_timed_out".equals(fieldName)) {
                    waitForTimedOut = parser.booleanValue();
                }
            } else if (currentToken == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            } else if (currentToken == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }

        final ClusterState clusterState = ClusterState.builder(clusterName).build();
        return new ClusterStateResponse(clusterName, clusterState, waitForTimedOut);
    }

    /**
     * Consumes the current object or array from the parser, including all nested structures.
     *
     * @param parser the parser positioned inside the object or array to skip
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
     * Builds the curl request for the cluster state API.
     *
     * @param request the cluster state request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final ClusterStateRequest request) {
        // RestClusterStateAction
        final StringBuilder buf = new StringBuilder();
        buf.append("/_cluster/state");

        final List<String> metrics = new ArrayList<>();
        if (request.routingTable()) {
            metrics.add("routing_table");
        }
        if (request.nodes()) {
            metrics.add("nodes");
        }
        if (request.metadata()) {
            metrics.add("metadata");
        }
        if (request.blocks()) {
            metrics.add("blocks");
        }
        if (request.customs()) {
            metrics.add("customs");
        }
        if (!metrics.isEmpty()) {
            buf.append("/").append(String.join(",", metrics));
        }

        if (request.indices() != null && request.indices().length > 0) {
            buf.append("/").append(UrlUtils.joinAndEncode(",", request.indices()));
        }

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.waitForMetadataVersion() != null) {
            curlRequest.param("wait_for_metadata_version", String.valueOf(request.waitForMetadataVersion()));
        }
        if (request.waitForTimeout() != null) {
            curlRequest.param("wait_for_timeout", request.waitForTimeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
