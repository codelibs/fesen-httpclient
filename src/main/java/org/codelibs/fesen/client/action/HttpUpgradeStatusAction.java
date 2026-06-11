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
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the upgrade status API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpUpgradeStatusAction extends HttpAction {

    /** The upgrade status action definition. */
    protected final UpgradeStatusAction action;

    /**
     * Creates a new HTTP upgrade status action.
     *
     * @param client the HTTP client used to send requests
     * @param action the upgrade status action definition
     */
    public HttpUpgradeStatusAction(final HttpClient client, final UpgradeStatusAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the upgrade status request and notifies the listener with the response.
     *
     * @param request the upgrade status request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final UpgradeStatusRequest request, final ActionListener<UpgradeStatusResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final UpgradeStatusResponse upgradeStatusResponse = fromXContent(parser);
                listener.onResponse(upgradeStatusResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses an upgrade status response, extracting the shard counts from the {@code _shards} section.
     * Since {@link UpgradeStatusResponse} has package-private constructors, the response is
     * reconstructed via the wire format.
     *
     * @param parser the parser positioned at the response content
     * @return the parsed upgrade status response
     * @throws IOException if parsing fails
     */
    protected UpgradeStatusResponse fromXContent(final XContentParser parser) throws IOException {
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
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                // Skip top-level number fields like size_in_bytes, size_to_upgrade_in_bytes
            }
        }

        // UpgradeStatusResponse has package-private constructors, so use ByteArrayStreamOutput
        // BroadcastResponse wire format: totalShards(int), successfulShards(int), failedShards(int), shardFailures(vint size)
        // Then UpgradeStatusResponse reads: ShardUpgradeStatus array(vint size)
        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeInt(totalShards);
            out.writeInt(successfulShards);
            out.writeInt(failedShards);
            out.writeVInt(0); // no shard failures
            out.writeVInt(0); // no ShardUpgradeStatus
            return action.getResponseReader().read(out.toStreamInput());
        }
    }

    /**
     * Consumes the current object from the parser, including all nested objects and arrays.
     *
     * @param parser the parser positioned inside the object to consume
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
     * Builds the curl request for the upgrade status API.
     *
     * @param request the upgrade status request
     * @return the curl request for the upgrade status endpoint
     */
    protected CurlRequest getCurlRequest(final UpgradeStatusRequest request) {
        // RestUpgradeStatusAction
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_upgrade");

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        return curlRequest;
    }
}
