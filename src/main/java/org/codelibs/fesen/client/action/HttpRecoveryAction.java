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
import org.opensearch.action.admin.indices.recovery.RecoveryAction;
import org.opensearch.action.admin.indices.recovery.RecoveryRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.XContentParser;

public class HttpRecoveryAction extends HttpAction {

    protected final RecoveryAction action;

    public HttpRecoveryAction(final HttpClient client, final RecoveryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RecoveryResponse recoveryResponse = fromXContent(parser);
                listener.onResponse(recoveryResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected RecoveryResponse fromXContent(final XContentParser parser) throws IOException {
        // Initialize parser - move to START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        // The recovery response JSON is {"indexName":{"shards":[{...}]}} with no _shards header.
        // RecoveryState is complex to parse, so we count shards and consume the rest.
        int totalShards = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                // index name
            } else if (token == XContentParser.Token.START_OBJECT) {
                totalShards += countShardsAndConsume(parser);
            }
        }

        return new RecoveryResponse(totalShards, totalShards, 0, Collections.emptyMap(), new ArrayList<>());
    }

    protected int countShardsAndConsume(final XContentParser parser) throws IOException {
        int shardCount = 0;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && "shards".equals(parser.currentName())) {
                token = parser.nextToken(); // START_ARRAY
                if (token == XContentParser.Token.START_ARRAY) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        shardCount++;
                        consumeObject(parser);
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            } else if (token == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }
        return shardCount;
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

    protected CurlRequest getCurlRequest(final RecoveryRequest request) {
        // RestRecoveryAction
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_recovery");

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        curlRequest.param("detailed", Boolean.toString(request.detailed()));
        curlRequest.param("active_only", Boolean.toString(request.activeOnly()));
        return curlRequest;
    }
}
