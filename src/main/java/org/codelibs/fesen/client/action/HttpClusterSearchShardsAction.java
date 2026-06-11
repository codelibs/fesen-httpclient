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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the cluster search shards API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpClusterSearchShardsAction extends HttpAction {

    /** The cluster search shards action. */
    protected final ClusterSearchShardsAction action;

    /**
     * Creates a new HTTP cluster search shards action.
     *
     * @param client the HTTP client
     * @param action the cluster search shards action
     */
    public HttpClusterSearchShardsAction(final HttpClient client, final ClusterSearchShardsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the cluster search shards request asynchronously.
     *
     * @param request the cluster search shards request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ClusterSearchShardsRequest request, final ActionListener<ClusterSearchShardsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterSearchShardsResponse searchShardsResponse = fromXContent(parser);
                listener.onResponse(searchShardsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a cluster search shards response from the given parser. The response body is consumed
     * and a response with empty groups, nodes, and alias filters is returned because the internal
     * structures cannot be reconstructed from JSON.
     *
     * @param parser the parser positioned at the response body
     * @return the cluster search shards response
     * @throws IOException if parsing fails
     */
    protected ClusterSearchShardsResponse fromXContent(final XContentParser parser) throws IOException {
        // ClusterSearchShardsResponse contains complex internal structures
        // (ShardRouting, AliasFilter) that are difficult to construct from JSON.
        // Return response with empty arrays/maps for basic compatibility.
        final XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            consumeObject(parser);
        }
        return new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0], new DiscoveryNode[0], Collections.emptyMap());
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
     * Builds the curl request for the cluster search shards API.
     *
     * @param request the cluster search shards request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final ClusterSearchShardsRequest request) {
        // RestClusterSearchShardsAction
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append("/").append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_search_shards");
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        return curlRequest;
    }
}
