/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Indicates ingestion failures at index and shard level.
 *
 * @param index the index
 * @param shard the shard
 * @param errorMessage the error message
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public record IngestionStateShardFailure(String index, int shard, String errorMessage) implements Writeable, ToXContentFragment {

    private static final String SHARD = "shard";
    private static final String ERROR = "error";

    /**
     * Creates a new IngestionStateShardFailure by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public IngestionStateShardFailure(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(errorMessage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shard);
        builder.field(ERROR, errorMessage);
        return builder.endObject();
    }

    /**
     * Groups provided shard ingestion state failures by index name.
     *
     * @param shardFailures the shard failures
     * @return the group shard failures by index
     */
    public static Map<String, List<IngestionStateShardFailure>> groupShardFailuresByIndex(IngestionStateShardFailure[] shardFailures) {
        Map<String, List<IngestionStateShardFailure>> shardFailuresByIndex = new HashMap<>();

        for (IngestionStateShardFailure shardFailure : shardFailures) {
            shardFailuresByIndex.computeIfAbsent(shardFailure.index(), (index) -> new ArrayList<>());
            shardFailuresByIndex.get(shardFailure.index()).add(shardFailure);
        }

        return shardFailuresByIndex;
    }
}
