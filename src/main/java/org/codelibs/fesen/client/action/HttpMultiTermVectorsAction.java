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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.action.termvectors.MultiTermVectorsAction;
import org.opensearch.action.termvectors.MultiTermVectorsItemResponse;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpMultiTermVectorsAction extends HttpAction {

    protected final MultiTermVectorsAction action;

    public HttpMultiTermVectorsAction(final HttpClient client, final MultiTermVectorsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MultiTermVectorsResponse multiResponse = fromXContent(parser);
                listener.onResponse(multiResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected MultiTermVectorsResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        final List<MultiTermVectorsItemResponse> items = new ArrayList<>();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY && "docs".equals(fieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        items.add(parseItem(parser));
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                consumeObject(parser);
            }
        }

        return new MultiTermVectorsResponse(items.toArray(new MultiTermVectorsItemResponse[0]));
    }

    protected MultiTermVectorsItemResponse parseItem(final XContentParser parser) throws IOException {
        String fieldName = null;
        String index = "";
        String id = "";
        boolean found = false;
        long took = 0;

        XContentParser.Token token;
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
                if ("took".equals(fieldName)) {
                    took = parser.longValue();
                } else if ("_version".equals(fieldName)) {
                    parser.longValue();
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

        final TermVectorsResponse tvResponse = new TermVectorsResponse(index, id);
        tvResponse.setExists(found);
        tvResponse.setTookInMillis(took);
        return new MultiTermVectorsItemResponse(tvResponse, null);
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

    protected CurlRequest getCurlRequest(final MultiTermVectorsRequest request) {
        // Determine common index if all requests share the same index
        final List<TermVectorsRequest> requests = request.getRequests();
        String commonIndex = null;
        if (!requests.isEmpty()) {
            commonIndex = requests.get(0).index();
            for (final TermVectorsRequest tvRequest : requests) {
                if (!commonIndex.equals(tvRequest.index())) {
                    commonIndex = null;
                    break;
                }
            }
        }

        final StringBuilder buf = new StringBuilder();
        if (commonIndex != null) {
            buf.append('/').append(UrlUtils.encode(commonIndex));
        }
        buf.append("/_mtermvectors");

        final CurlRequest curlRequest = client.getCurlRequest(POST, buf.toString());

        // Build request body with docs using XContentBuilder for safe JSON construction
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startArray("docs");
            for (final TermVectorsRequest tvRequest : requests) {
                builder.startObject();
                builder.field("_index", tvRequest.index());
                if (tvRequest.id() != null) {
                    builder.field("_id", tvRequest.id());
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            curlRequest.body(builder.toString());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to build request body", e);
        }

        return curlRequest;
    }
}
