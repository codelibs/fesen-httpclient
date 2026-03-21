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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.XContentParser;

public class HttpGetIngestionStateAction extends HttpAction {

    protected final GetIngestionStateAction action;

    public HttpGetIngestionStateAction(final HttpClient client, final GetIngestionStateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIngestionStateRequest request, final ActionListener<GetIngestionStateResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIngestionStateResponse getIngestionStateResponse = fromXContent(parser);
                listener.onResponse(getIngestionStateResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected GetIngestionStateResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_shards".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("total".equals(fieldName)) {
                                totalShards = parser.intValue();
                            } else if ("successful".equals(fieldName)) {
                                successfulShards = parser.intValue();
                            } else if ("failed".equals(fieldName)) {
                                failedShards = parser.intValue();
                            }
                        }
                    }
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }

        return new GetIngestionStateResponse(new ShardIngestionState[0], totalShards, successfulShards, failedShards, null, shardFailures);
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

    protected CurlRequest getCurlRequest(final GetIngestionStateRequest request) {
        // RestGetIngestionStateAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/ingestion/_state", request.indices());
        if (request.getShards().length > 0) {
            curlRequest.param("shards", IntStream.of(request.getShards()).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
