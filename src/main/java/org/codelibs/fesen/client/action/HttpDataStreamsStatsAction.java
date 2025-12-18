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
import org.opensearch.action.admin.indices.datastream.DataStreamsStatsAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentParser;

public class HttpDataStreamsStatsAction extends HttpAction {

    protected DataStreamsStatsAction action;

    public HttpDataStreamsStatsAction(final HttpClient client, final DataStreamsStatsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DataStreamsStatsAction.Request request, final ActionListener<DataStreamsStatsAction.Response> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DataStreamsStatsAction.Response dataStreamsStatsResponse = fromXContent(parser);
                listener.onResponse(dataStreamsStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected DataStreamsStatsAction.Response fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        int dataStreamCount = 0;
        int backingIndices = 0;
        long totalStoreSizeBytes = 0;
        final List<DataStreamsStatsAction.DataStreamStats> dataStreams = new ArrayList<>();
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

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
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("data_streams".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.START_OBJECT) {
                            dataStreams.add(parseDataStreamStats(parser));
                        }
                    }
                } else {
                    consumeArray(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("data_stream_count".equals(fieldName)) {
                    dataStreamCount = parser.intValue();
                } else if ("backing_indices".equals(fieldName)) {
                    backingIndices = parser.intValue();
                } else if ("total_store_size_bytes".equals(fieldName)) {
                    totalStoreSizeBytes = parser.longValue();
                }
            }
        }

        return new DataStreamsStatsAction.Response(totalShards, successfulShards, failedShards, shardFailures, dataStreamCount,
                backingIndices, new ByteSizeValue(totalStoreSizeBytes), dataStreams.toArray(new DataStreamsStatsAction.DataStreamStats[0]));
    }

    protected DataStreamsStatsAction.DataStreamStats parseDataStreamStats(final XContentParser parser) throws IOException {
        String fieldName = null;
        String dataStream = null;
        int backingIndices = 0;
        long storeSizeBytes = 0;
        long maximumTimestamp = 0;

        parser.nextToken();
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("data_stream".equals(fieldName)) {
                    dataStream = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("backing_indices".equals(fieldName)) {
                    backingIndices = parser.intValue();
                } else if ("store_size_bytes".equals(fieldName)) {
                    storeSizeBytes = parser.longValue();
                } else if ("maximum_timestamp".equals(fieldName)) {
                    maximumTimestamp = parser.longValue();
                }
            }
            parser.nextToken();
        }

        return new DataStreamsStatsAction.DataStreamStats(dataStream, backingIndices, new ByteSizeValue(storeSizeBytes), maximumTimestamp);
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

    protected void consumeArray(final XContentParser parser) throws IOException {
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

    protected CurlRequest getCurlRequest(final DataStreamsStatsAction.Request request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_data_stream");
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_stats");
        return client.getCurlRequest(GET, buf.toString());
    }
}
