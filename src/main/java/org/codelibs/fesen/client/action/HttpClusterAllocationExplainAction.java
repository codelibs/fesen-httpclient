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

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;

public class HttpClusterAllocationExplainAction extends HttpAction {

    protected final ClusterAllocationExplainAction action;

    public HttpClusterAllocationExplainAction(final HttpClient client, final ClusterAllocationExplainAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterAllocationExplainRequest request, final ActionListener<ClusterAllocationExplainResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterAllocationExplainResponse explainResponse = fromXContent(parser);
                listener.onResponse(explainResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected ClusterAllocationExplainResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        String index = "";
        int shard = 0;
        boolean primary = true;
        String currentNodeId = null;
        String currentNodeName = null;

        final XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        XContentParser.Token currentToken;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (currentToken == XContentParser.Token.VALUE_STRING) {
                if ("index".equals(fieldName)) {
                    index = parser.text();
                }
            } else if (currentToken == XContentParser.Token.VALUE_NUMBER) {
                if ("shard".equals(fieldName)) {
                    shard = parser.intValue();
                }
            } else if (currentToken == XContentParser.Token.VALUE_BOOLEAN) {
                if ("primary".equals(fieldName)) {
                    primary = parser.booleanValue();
                }
            } else if (currentToken == XContentParser.Token.START_OBJECT) {
                if ("current_node".equals(fieldName)) {
                    while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (currentToken == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (currentToken == XContentParser.Token.VALUE_STRING) {
                            if ("id".equals(fieldName)) {
                                currentNodeId = parser.text();
                            } else if ("name".equals(fieldName)) {
                                currentNodeName = parser.text();
                            }
                        } else if (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY) {
                            consumeObject(parser);
                        }
                    }
                } else {
                    consumeObject(parser);
                }
            } else if (currentToken == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }

        final ShardId shardId = new ShardId(new Index(index, "_na_"), shard);
        final ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, primary, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));

        final DiscoveryNode currentNode =
                currentNodeId != null
                        ? new DiscoveryNode(currentNodeName != null ? currentNodeName : currentNodeId, currentNodeId,
                                new TransportAddress(TransportAddress.META_ADDRESS, 0), Collections.emptyMap(), Collections.emptySet(),
                                Version.CURRENT)
                        : null;

        final ClusterAllocationExplanation explanation =
                new ClusterAllocationExplanation(shardRouting, currentNode, null, ClusterInfo.EMPTY, ShardAllocationDecision.NOT_TAKEN);

        return new ClusterAllocationExplainResponse(explanation);
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

    protected CurlRequest getCurlRequest(final ClusterAllocationExplainRequest request) {
        final CurlRequest curlRequest;
        if (request.getIndex() != null) {
            curlRequest = client.getCurlRequest(POST, "/_cluster/allocation/explain");
            try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                builder.field("index", request.getIndex());
                builder.field("shard", request.getShard());
                builder.field("primary", request.isPrimary());
                builder.endObject();
                curlRequest.body(builder.toString());
            } catch (final IOException e) {
                throw new RuntimeException("Failed to build request body", e);
            }
        } else {
            curlRequest = client.getCurlRequest(GET, "/_cluster/allocation/explain");
        }
        if (request.includeYesDecisions()) {
            curlRequest.param("include_yes_decisions", "true");
        }
        if (request.includeDiskInfo()) {
            curlRequest.param("include_disk_info", "true");
        }
        return curlRequest;
    }
}
