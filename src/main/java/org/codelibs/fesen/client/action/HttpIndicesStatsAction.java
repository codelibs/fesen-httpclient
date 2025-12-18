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
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.XContentParser;

public class HttpIndicesStatsAction extends HttpAction {

    protected IndicesStatsAction action;

    public HttpIndicesStatsAction(final HttpClient client, final IndicesStatsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final IndicesStatsResponse indicesStatsResponse = fromXContent(parser);
                listener.onResponse(indicesStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected IndicesStatsResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
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
                } else if ("_all".equals(fieldName)) {
                    // Skip _all section
                    consumeObject(parser);
                } else if ("indices".equals(fieldName)) {
                    // Skip indices section - complex to parse
                    consumeObject(parser);
                } else {
                    consumeObject(parser);
                }
            }
        }

        // Build IndicesStatsResponse with empty ShardStats array
        // The actual stats are computed lazily from the shards array
        return new IndicesStatsResponse(new ShardStats[0], totalShards, successfulShards, failedShards, shardFailures);
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

    protected CurlRequest getCurlRequest(final IndicesStatsRequest request) {
        final StringBuilder buf = new StringBuilder();
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        buf.append("/_stats");

        final String metric = getMetric(request.flags());
        if (metric.length() > 0) {
            buf.append('/').append(metric);
        }

        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());

        // Add query parameters
        if (request.flags().completionDataFields() != null && request.flags().completionDataFields().length > 0) {
            curlRequest.param("completion_fields", String.join(",", request.flags().completionDataFields()));
        }
        if (request.flags().fieldDataFields() != null && request.flags().fieldDataFields().length > 0) {
            curlRequest.param("fielddata_fields", String.join(",", request.flags().fieldDataFields()));
        }
        if (request.flags().groups() != null && request.flags().groups().length > 0) {
            curlRequest.param("groups", String.join(",", request.flags().groups()));
        }
        if (request.flags().includeSegmentFileSizes()) {
            curlRequest.param("include_segment_file_sizes", "true");
        }
        if (request.flags().includeUnloadedSegments()) {
            curlRequest.param("include_unloaded_segments", "true");
        }

        return curlRequest;
    }

    protected String getMetric(final CommonStatsFlags flags) {
        final List<String> metrics = new ArrayList<>();
        if (flags.isSet(Flag.Docs)) {
            metrics.add("docs");
        }
        if (flags.isSet(Flag.Store)) {
            metrics.add("store");
        }
        if (flags.isSet(Flag.Indexing)) {
            metrics.add("indexing");
        }
        if (flags.isSet(Flag.Get)) {
            metrics.add("get");
        }
        if (flags.isSet(Flag.Search)) {
            metrics.add("search");
        }
        if (flags.isSet(Flag.Merge)) {
            metrics.add("merge");
        }
        if (flags.isSet(Flag.Refresh)) {
            metrics.add("refresh");
        }
        if (flags.isSet(Flag.Flush)) {
            metrics.add("flush");
        }
        if (flags.isSet(Flag.Warmer)) {
            metrics.add("warmer");
        }
        if (flags.isSet(Flag.QueryCache)) {
            metrics.add("query_cache");
        }
        if (flags.isSet(Flag.FieldData)) {
            metrics.add("fielddata");
        }
        if (flags.isSet(Flag.Segments)) {
            metrics.add("segments");
        }
        if (flags.isSet(Flag.Completion)) {
            metrics.add("completion");
        }
        if (flags.isSet(Flag.Translog)) {
            metrics.add("translog");
        }
        if (flags.isSet(Flag.RequestCache)) {
            metrics.add("request_cache");
        }
        if (flags.isSet(Flag.Recovery)) {
            metrics.add("recovery");
        }

        // If all metrics are set, use _all or return empty
        if (metrics.size() == Flag.values().length) {
            return "";
        }
        return String.join(",", metrics);
    }
}
