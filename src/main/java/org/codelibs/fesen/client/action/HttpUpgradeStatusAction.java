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
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
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
     * Parses an {@code _upgrade} status response and reconstructs an {@link UpgradeStatusResponse}.
     *
     * <p>The {@code _upgrade} API response is aggregate-only: it exposes per-index byte counts
     * ({@code size_in_bytes}, {@code size_to_upgrade_in_bytes}, {@code size_to_upgrade_ancient_in_bytes})
     * but no shard/routing data ({@code detailed=true} is rejected by the server). Since
     * {@link UpgradeStatusResponse} has package-private constructors and derives its byte getters
     * from a {@code ShardUpgradeStatus[]}, the response is reconstructed via the wire format by
     * synthesizing exactly one shard per index carrying that index's aggregate bytes. The synthetic
     * shard routing is therefore artificial (a single unassigned primary); only the byte totals are
     * meaningful, because the API exposes no real per-shard routing.
     *
     * @param parser the parser positioned at the response content
     * @return the parsed upgrade status response
     * @throws IOException if parsing fails
     */
    protected UpgradeStatusResponse fromXContent(final XContentParser parser) throws IOException {
        final List<IndexUpgradeBytes> indexStats = new ArrayList<>();
        String fieldName = null;

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
                if ("indices".equals(fieldName)) {
                    parseIndices(parser, indexStats);
                } else {
                    consumeObject(parser);
                }
            }
            // Top-level size_in_bytes/size_to_upgrade_* numbers are re-derived from the
            // per-index aggregates below, so they are intentionally ignored here.
        }

        return toResponse(indexStats);
    }

    /**
     * Parses the {@code indices} object, collecting the aggregate byte counts of each index.
     *
     * @param parser the parser positioned at the START_OBJECT of the {@code indices} value
     * @param indexStats the list to append the per-index aggregates to
     * @throws IOException if parsing fails
     */
    protected void parseIndices(final XContentParser parser, final List<IndexUpgradeBytes> indexStats) throws IOException {
        String indexName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                indexName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                indexStats.add(parseIndexBytes(indexName, parser));
            }
        }
    }

    /**
     * Parses a single index's aggregate byte counts.
     *
     * @param indexName the name of the index being parsed
     * @param parser the parser positioned at the START_OBJECT of the index's stats
     * @return the aggregate byte counts of the index
     * @throws IOException if parsing fails
     */
    protected IndexUpgradeBytes parseIndexBytes(final String indexName, final XContentParser parser) throws IOException {
        long totalBytes = 0;
        long toUpgradeBytes = 0;
        long toUpgradeBytesAncient = 0;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size_in_bytes".equals(fieldName)) {
                    totalBytes = parser.longValue();
                } else if ("size_to_upgrade_in_bytes".equals(fieldName)) {
                    toUpgradeBytes = parser.longValue();
                } else if ("size_to_upgrade_ancient_in_bytes".equals(fieldName)) {
                    toUpgradeBytesAncient = parser.longValue();
                }
            }
        }
        return new IndexUpgradeBytes(indexName, totalBytes, toUpgradeBytes, toUpgradeBytesAncient);
    }

    /**
     * Reconstructs an {@link UpgradeStatusResponse} from the collected per-index aggregates by
     * synthesizing one shard per index over the wire format that
     * {@link UpgradeStatusResponse#UpgradeStatusResponse(org.opensearch.core.common.io.stream.StreamInput)}
     * reads. The synthetic routing is artificial (a single unassigned primary); only the byte totals
     * are meaningful because the {@code _upgrade} API exposes no per-shard routing.
     *
     * @param indexStats the per-index aggregate byte counts (may be empty)
     * @return the reconstructed upgrade status response
     * @throws IOException if serialization fails
     */
    protected UpgradeStatusResponse toResponse(final List<IndexUpgradeBytes> indexStats) throws IOException {
        final int shardCount = indexStats.size();
        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            // BroadcastResponse header (read via readVInt): totalShards, successfulShards, failedShards,
            // shardFailures length. One synthetic shard per index, all successful, none failed.
            out.writeVInt(shardCount);
            out.writeVInt(shardCount);
            out.writeVInt(0);
            out.writeVInt(0); // no shard failures
            // UpgradeStatusResponse: the number of ShardUpgradeStatus entries.
            out.writeVInt(shardCount);
            for (final IndexUpgradeBytes stats : indexStats) {
                // Synthesize exactly one shard per index carrying that index's aggregate bytes. The
                // shard id and routing share the same index name so getIndices() groups them correctly.
                final ShardId shardId = new ShardId(new Index(stats.index, "_na_"), 0);
                final ShardRouting shardRouting =
                        ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                                new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "synthetic"));
                // ShardUpgradeStatus wire format: shardId, shardRouting, then three fixed-width longs.
                shardId.writeTo(out);
                shardRouting.writeTo(out);
                out.writeLong(stats.totalBytes);
                out.writeLong(stats.toUpgradeBytes);
                out.writeLong(stats.toUpgradeBytesAncient);
            }
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
     * Holds the aggregate upgrade byte counts parsed for a single index. These are the only
     * per-index values the {@code _upgrade} API exposes; no shard/routing data is available.
     */
    protected static final class IndexUpgradeBytes {
        /** The index name. */
        protected final String index;
        /** The total size of the index in bytes. */
        protected final long totalBytes;
        /** The number of bytes that need to be upgraded. */
        protected final long toUpgradeBytes;
        /** The number of ancient bytes that need to be upgraded. */
        protected final long toUpgradeBytesAncient;

        /**
         * Creates a new aggregate byte holder for an index.
         *
         * @param index the index name
         * @param totalBytes the total size of the index in bytes
         * @param toUpgradeBytes the number of bytes that need to be upgraded
         * @param toUpgradeBytesAncient the number of ancient bytes that need to be upgraded
         */
        protected IndexUpgradeBytes(final String index, final long totalBytes, final long toUpgradeBytes,
                final long toUpgradeBytesAncient) {
            this.index = index;
            this.totalBytes = totalBytes;
            this.toUpgradeBytes = toUpgradeBytes;
            this.toUpgradeBytesAncient = toUpgradeBytesAncient;
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
