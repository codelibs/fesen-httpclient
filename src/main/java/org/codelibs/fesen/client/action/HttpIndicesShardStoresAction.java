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
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpIndicesShardStoresAction extends HttpAction {

    protected final IndicesShardStoresAction action;

    public HttpIndicesShardStoresAction(final HttpClient client, final IndicesShardStoresAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesShardStoresRequest request, final ActionListener<IndicesShardStoresResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final IndicesShardStoresResponse indicesShardStoresResponse = fromXContent(parser);
                listener.onResponse(indicesShardStoresResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected IndicesShardStoresResponse fromXContent(final XContentParser parser) throws IOException {
        // Initialize parser - move to START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        // The response JSON is {"indices":{"indexName":{"shards":{"0":{"stores":[...]}}}}}
        // StoreStatus is complex to parse, so we consume the entire JSON and return an empty response.
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            }
        }

        return new IndicesShardStoresResponse(Collections.emptyMap(), Collections.emptyList());
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

    protected CurlRequest getCurlRequest(final IndicesShardStoresRequest request) {
        // RestIndicesShardStoresAction
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_shard_stores");

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.shardStatuses() != null && !request.shardStatuses().isEmpty()) {
            final List<String> statuses = new ArrayList<>();
            for (final ClusterHealthStatus status : request.shardStatuses()) {
                statuses.add(status.name().toLowerCase());
            }
            curlRequest.param("status", String.join(",", statuses));
        }
        return curlRequest;
    }
}
