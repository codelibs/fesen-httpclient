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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequest;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreShardMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.XContentParser;

public class HttpRemoteStoreMetadataAction extends HttpAction {

    protected RemoteStoreMetadataAction action;

    public HttpRemoteStoreMetadataAction(final HttpClient client, final RemoteStoreMetadataAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RemoteStoreMetadataRequest request, final ActionListener<RemoteStoreMetadataResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RemoteStoreMetadataResponse remoteStoreMetadataResponse = fromXContent(parser);
                listener.onResponse(remoteStoreMetadataResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected RemoteStoreMetadataResponse fromXContent(final XContentParser parser) throws IOException {
        String fieldName = null;
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final List<RemoteStoreShardMetadata> metadataList = new ArrayList<>();

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
                } else if ("indices".equals(fieldName)) {
                    parseIndices(parser, metadataList);
                } else {
                    consumeObject(parser);
                }
            }
        }

        return new RemoteStoreMetadataResponse(metadataList.toArray(new RemoteStoreShardMetadata[0]), totalShards, successfulShards,
                failedShards, shardFailures);
    }

    @SuppressWarnings("unchecked")
    protected void parseIndices(final XContentParser parser, final List<RemoteStoreShardMetadata> metadataList) throws IOException {
        // indices: { "index_name": { "shards": { "0": [ {shard metadata...} ] } } }
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String indexName = parser.currentName();
                parser.nextToken(); // START_OBJECT for index
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME && "shards".equals(parser.currentName())) {
                        parser.nextToken(); // START_OBJECT for shards
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                final int shardId = Integer.parseInt(parser.currentName());
                                parser.nextToken(); // START_ARRAY
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.START_OBJECT) {
                                        metadataList.add(parseShardMetadata(parser, indexName, shardId));
                                    }
                                }
                            }
                        }
                    } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                        consumeObject(parser);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected RemoteStoreShardMetadata parseShardMetadata(final XContentParser parser, final String indexName, final int shardId)
            throws IOException {
        String latestSegmentMetadataFileName = null;
        String latestTranslogMetadataFileName = null;
        Map<String, Map<String, Object>> segmentMetadataFiles = new HashMap<>();
        Map<String, Map<String, Object>> translogMetadataFiles = new HashMap<>();

        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("latest_segment_metadata_filename".equals(fieldName)) {
                    latestSegmentMetadataFileName = parser.text();
                } else if ("latest_translog_metadata_filename".equals(fieldName)) {
                    latestTranslogMetadataFileName = parser.text();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("available_segment_metadata_files".equals(fieldName)) {
                    segmentMetadataFiles = parseMetadataFiles(parser);
                } else if ("available_translog_metadata_files".equals(fieldName)) {
                    translogMetadataFiles = parseMetadataFiles(parser);
                } else {
                    consumeObject(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                consumeObject(parser);
            }
        }

        return new RemoteStoreShardMetadata(indexName, shardId, segmentMetadataFiles, translogMetadataFiles, latestSegmentMetadataFileName,
                latestTranslogMetadataFileName);
    }

    protected Map<String, Map<String, Object>> parseMetadataFiles(final XContentParser parser) throws IOException {
        final Map<String, Map<String, Object>> files = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String fileName = parser.currentName();
                parser.nextToken(); // START_OBJECT
                files.put(fileName, parser.map());
            }
        }
        return files;
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

    protected CurlRequest getCurlRequest(final RemoteStoreMetadataRequest request) {
        final StringBuilder buf = new StringBuilder();
        buf.append("/_remotestore/metadata");
        if (request.indices() != null && request.indices().length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", request.indices()));
        }
        if (request.shards() != null && request.shards().length > 0) {
            buf.append('/').append(String.join(",", request.shards()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        return curlRequest;
    }
}
