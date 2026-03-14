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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpUpgradeAction extends HttpAction {

    protected final UpgradeAction action;

    public HttpUpgradeAction(final HttpClient client, final UpgradeAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final UpgradeResponse upgradeResponse = fromXContent(parser);
                listener.onResponse(upgradeResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected UpgradeResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;

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
                } else if ("upgraded_indices".equals(fieldName)) {
                    consumeObject(parser);
                } else {
                    consumeObject(parser);
                }
            }
        }

        // UpgradeResponse has package-private constructors, so use ByteArrayStreamOutput
        // BroadcastResponse wire format: totalShards(int), successfulShards(int), failedShards(int), shardFailures(vint size)
        // Then UpgradeResponse reads: upgraded versions map(vint size)
        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeInt(totalShards);
            out.writeInt(successfulShards);
            out.writeInt(failedShards);
            out.writeVInt(0); // no shard failures
            out.writeVInt(0); // no upgraded indices
            return action.getResponseReader().read(out.toStreamInput());
        }
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

    protected CurlRequest getCurlRequest(final UpgradeRequest request) {
        // RestUpgradeAction
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_upgrade");

        final CurlRequest curlRequest = client.getCurlRequest(POST, buf.toString());
        curlRequest.param("only_ancient_segments", Boolean.toString(request.upgradeOnlyAncientSegments()));
        return curlRequest;
    }
}
