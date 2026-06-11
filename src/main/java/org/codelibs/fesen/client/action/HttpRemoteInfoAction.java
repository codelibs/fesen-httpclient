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
import org.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.opensearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.opensearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the remote cluster info API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpRemoteInfoAction extends HttpAction {

    /** The remote info action. */
    protected final RemoteInfoAction action;

    /**
     * Creates a new instance.
     *
     * @param client the HTTP client
     * @param action the remote info action
     */
    public HttpRemoteInfoAction(final HttpClient client, final RemoteInfoAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the remote info request and notifies the listener with the response.
     *
     * @param request the remote info request
     * @param listener the listener to be notified with the remote info response or a failure
     */
    public void execute(final RemoteInfoRequest request, final ActionListener<RemoteInfoResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RemoteInfoResponse remoteInfoResponse = fromXContent(parser);
                listener.onResponse(remoteInfoResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses a remote info response from the given parser. The response body is consumed and an
     * empty connection list is returned because RemoteConnectionInfo cannot be constructed externally.
     *
     * @param parser the content parser
     * @return a remote info response with an empty connection list
     * @throws IOException if parsing fails
     */
    protected RemoteInfoResponse fromXContent(final XContentParser parser) throws IOException {
        // RemoteConnectionInfo requires complex ModeInfo construction
        // Return empty list - callers can use the REST API directly for full details
        final XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            consumeObject(parser);
        }
        return new RemoteInfoResponse(Collections.emptyList());
    }

    /**
     * Consumes the current object or array from the parser, including all nested structures.
     *
     * @param parser the content parser
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
     * Builds a curl request for the remote info request.
     *
     * @param request the remote info request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final RemoteInfoRequest request) {
        return client.getCurlRequest(GET, "/_remote/info");
    }
}
