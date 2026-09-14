/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion;

import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport response for ingestion state updates.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class IngestionUpdateStateResponse extends AcknowledgedResponse {
    /**
     * The SHARD_ACK constant.
     */
    protected static final String SHARD_ACK = "shards_acknowledged";
    /**
     * The ERROR constant.
     */
    protected static final String ERROR = "error";
    /**
     * The FAILURES constant.
     */
    protected static final String FAILURES = "failures";

    /**
     * The shards acknowledged.
     */
    protected boolean shardsAcknowledged;
    /**
     * The shard failures list.
     */
    protected IngestionStateShardFailure[] shardFailuresList;
    /**
     * The error message.
     */
    protected String errorMessage;

    /**
     * Creates a new IngestionUpdateStateResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public IngestionUpdateStateResponse(StreamInput in) throws IOException {
        super(in);
        shardFailuresList = in.readArray(IngestionStateShardFailure::new, IngestionStateShardFailure[]::new);
        errorMessage = in.readString();
        shardsAcknowledged = in.readBoolean();
    }

    /**
     * Creates a new IngestionUpdateStateResponse.
     *
     * @param acknowledged the acknowledged
     * @param shardsAcknowledged the shards acknowledged
     * @param shardFailuresList the shard failures list
     * @param errorMessage the error message
     */
    public IngestionUpdateStateResponse(
        final boolean acknowledged,
        final boolean shardsAcknowledged,
        final IngestionStateShardFailure[] shardFailuresList,
        String errorMessage
    ) {
        super(acknowledged);
        this.shardFailuresList = shardFailuresList;
        this.shardsAcknowledged = shardsAcknowledged;
        this.errorMessage = errorMessage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shardFailuresList);
        out.writeString(errorMessage);
        out.writeBoolean(shardsAcknowledged);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(SHARD_ACK, shardsAcknowledged);

        if (Strings.isEmpty(errorMessage) == false) {
            builder.field(ERROR, errorMessage);
        }

        if (shardFailuresList.length > 0) {
            Map<String, List<IngestionStateShardFailure>> shardFailuresByIndex = IngestionStateShardFailure.groupShardFailuresByIndex(
                shardFailuresList
            );
            builder.startObject(FAILURES);
            for (Map.Entry<String, List<IngestionStateShardFailure>> indexShardFailures : shardFailuresByIndex.entrySet()) {
                builder.startArray(indexShardFailures.getKey());
                for (IngestionStateShardFailure shardFailure : indexShardFailures.getValue()) {
                    shardFailure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Returns the shards acknowledged flag.
     *
     * @return the shards acknowledged flag
     */
    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }
}
