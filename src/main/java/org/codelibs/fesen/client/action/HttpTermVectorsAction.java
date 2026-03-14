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
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.termvectors.TermVectorsAction;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpTermVectorsAction extends HttpAction {

    protected final TermVectorsAction action;

    public HttpTermVectorsAction(final HttpClient client, final TermVectorsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final TermVectorsRequest request, final ActionListener<TermVectorsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final TermVectorsResponse termVectorsResponse = fromXContent(parser);
                listener.onResponse(termVectorsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected TermVectorsResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        String index = "";
        String id = "";
        boolean found = false;
        long took = 0;

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("_index".equals(fieldName)) {
                    index = parser.text();
                } else if ("_id".equals(fieldName)) {
                    id = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("_version".equals(fieldName)) {
                    parser.longValue(); // consume but don't use
                } else if ("took".equals(fieldName)) {
                    took = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("found".equals(fieldName)) {
                    found = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            } else if (token == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }

        final TermVectorsResponse response = new TermVectorsResponse(index, id);
        response.setExists(found);
        response.setTookInMillis(took);
        return response;
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

    protected CurlRequest getCurlRequest(final TermVectorsRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append('/').append(UrlUtils.encode(request.index())).append("/_termvectors");
        if (request.id() != null && !request.id().isEmpty()) {
            buf.append('/').append(UrlUtils.encode(request.id()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.selectedFields() != null && request.selectedFields().size() > 0) {
            curlRequest.param("fields", String.join(",", request.selectedFields()));
        }
        if (!request.offsets()) {
            curlRequest.param("offsets", "false");
        }
        if (!request.positions()) {
            curlRequest.param("positions", "false");
        }
        if (!request.payloads()) {
            curlRequest.param("payloads", "false");
        }
        if (!request.fieldStatistics()) {
            curlRequest.param("field_statistics", "false");
        }
        if (request.termStatistics()) {
            curlRequest.param("term_statistics", "true");
        }
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        if (!request.realtime()) {
            curlRequest.param("realtime", "false");
        }
        return curlRequest;
    }
}
