/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastResponse;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Transport response for retrieving ingestion state.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class GetIngestionStateResponse extends BroadcastResponse {
    private static final String INGESTION_STATE = "ingestion_state";
    private static final String NEXT_PAGE_TOKEN = "next_page_token";

    private ShardIngestionState[] shardStates;
    @Nullable
    private String nextPageToken;

    /**
     * Creates a new GetIngestionStateResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public GetIngestionStateResponse(StreamInput in) throws IOException {
        super(in);
        shardStates = in.readArray(ShardIngestionState::new, ShardIngestionState[]::new);
        nextPageToken = in.readOptionalString();
    }

    /**
     * Creates a new GetIngestionStateResponse.
     *
     * @param shardStates the shard states
     * @param totalShards the total shards
     * @param successfulShards the successful shards
     * @param failedShards the failed shards
     * @param nextPageToken the next page token
     * @param shardFailures the shard failures
     */
    public GetIngestionStateResponse(
        ShardIngestionState[] shardStates,
        int totalShards,
        int successfulShards,
        int failedShards,
        @Nullable String nextPageToken,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardStates = shardStates;
        this.nextPageToken = nextPageToken;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shardStates);
        out.writeOptionalString(nextPageToken);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomXContentFields(builder, params);
        if (Strings.isEmpty(nextPageToken) == false) {
            builder.field(NEXT_PAGE_TOKEN, nextPageToken);
        }

        Map<String, List<ShardIngestionState>> shardStateByIndex = ShardIngestionState.groupShardStateByIndex(shardStates);
        builder.startObject(INGESTION_STATE);

        for (Map.Entry<String, List<ShardIngestionState>> indexShardIngestionStateEntry : shardStateByIndex.entrySet()) {
            builder.startArray(indexShardIngestionStateEntry.getKey());
            indexShardIngestionStateEntry.getValue().sort(Comparator.comparingInt(ShardIngestionState::getShardId));
            for (ShardIngestionState shardIngestionState : indexShardIngestionStateEntry.getValue()) {
                shardIngestionState.toXContent(builder, params);
            }
            builder.endArray();
        }

        builder.endObject();
    }

    /**
     * Returns the shard states.
     *
     * @return the shard states
     */
    public ShardIngestionState[] getShardStates() {
        return shardStates;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, false);
    }

    /**
     * Returns the next page token.
     *
     * @return the next page token
     */
    public String getNextPageToken() {
        return nextPageToken;
    }
}
