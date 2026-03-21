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
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionAction;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpResumeIngestionAction extends HttpAction {

    protected final ResumeIngestionAction action;

    public HttpResumeIngestionAction(final HttpClient client, final ResumeIngestionAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ResumeIngestionRequest request, final ActionListener<ResumeIngestionResponse> listener) {
        String body = null;
        if (request.getResetSettings().length > 0) {
            try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                builder.startArray("reset_settings");
                for (final ResumeIngestionRequest.ResetSettings rs : request.getResetSettings()) {
                    builder.startObject();
                    builder.field("shard", rs.getShard());
                    builder.field("mode", rs.getMode().name().toLowerCase(Locale.ROOT));
                    builder.field("value", rs.getValue());
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                builder.flush();
                body = BytesReference.bytes(builder).utf8ToString();
            } catch (final IOException e) {
                throw new OpenSearchException("Failed to parse a request.", e);
            }
        }
        final CurlRequest curlRequest = getCurlRequest(request);
        if (body != null) {
            curlRequest.body(body);
        }
        curlRequest.execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ResumeIngestionResponse resumeIngestionResponse = fromXContent(parser);
                listener.onResponse(resumeIngestionResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected ResumeIngestionResponse fromXContent(final XContentParser parser) throws IOException {
        boolean acknowledged = false;
        boolean shardsAcknowledged = false;

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected START_OBJECT but got " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String field = parser.currentName();
                parser.nextToken();
                if ("acknowledged".equals(field)) {
                    acknowledged = parser.booleanValue();
                } else if ("shards_acknowledged".equals(field)) {
                    shardsAcknowledged = parser.booleanValue();
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    consumeObject(parser);
                }
            }
        }

        return new ResumeIngestionResponse(acknowledged, shardsAcknowledged, new IngestionStateShardFailure[0], "");
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

    protected CurlRequest getCurlRequest(final ResumeIngestionRequest request) {
        // RestResumeIngestionAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/ingestion/_resume", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.clusterManagerNodeTimeout() != null) {
            curlRequest.param("cluster_manager_timeout", request.clusterManagerNodeTimeout().toString());
        }
        return curlRequest;
    }
}
