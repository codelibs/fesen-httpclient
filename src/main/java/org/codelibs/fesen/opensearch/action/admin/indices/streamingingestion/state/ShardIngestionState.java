/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.Nullable;
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
 * Represents ingestion shard state.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class ShardIngestionState implements Writeable, ToXContentFragment {
    private static final String SHARD = "shard";
    private static final String POLLER_STATE = "poller_state";
    private static final String ERROR_POLICY = "error_policy";
    private static final String POLLER_PAUSED = "poller_paused";
    private static final String WRITE_BLOCK_ENABLED = "write_block_enabled";
    private static final String BATCH_START_POINTER = "batch_start_pointer";
    private static final String IS_PRIMARY = "is_primary";
    private static final String NODE_NAME = "node";

    private String index;
    private int shardId;
    private String pollerState;
    private String errorPolicy;
    private boolean isPollerPaused;
    boolean isWriteBlockEnabled;
    private String batchStartPointer;
    private boolean isPrimary;
    private String nodeName;

    /**
     * Creates a new ShardIngestionState.
     */
    public ShardIngestionState() {
        this("", -1, "", "", false, false, "", true, "");
    }

    /**
     * Creates a new ShardIngestionState by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ShardIngestionState(StreamInput in) throws IOException {
        this.index = in.readString();
        this.shardId = in.readVInt();
        this.pollerState = in.readOptionalString();
        this.errorPolicy = in.readOptionalString();
        this.isPollerPaused = in.readBoolean();
        this.isWriteBlockEnabled = in.readBoolean();
        this.batchStartPointer = in.readString();

        if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
            this.isPrimary = in.readBoolean();
            this.nodeName = in.readString();
        } else {
            // added from version 3.3 onwards
            this.isPrimary = true;
            this.nodeName = "";
        }
    }

    /**
     * Creates a new ShardIngestionState.
     *
     * @param index the index
     * @param shardId the shard identifier
     * @param pollerState the poller state
     * @param errorPolicy the error policy
     * @param isPollerPaused the is poller paused
     * @param isWriteBlockEnabled the is write block enabled
     * @param batchStartPointer the batch start pointer
     * @param isPrimary the is primary
     * @param nodeName the node name
     */
    public ShardIngestionState(
        String index,
        int shardId,
        @Nullable String pollerState,
        @Nullable String errorPolicy,
        boolean isPollerPaused,
        boolean isWriteBlockEnabled,
        String batchStartPointer,
        boolean isPrimary,
        String nodeName
    ) {
        this.index = index;
        this.shardId = shardId;
        this.pollerState = pollerState;
        this.errorPolicy = errorPolicy;
        this.isPollerPaused = isPollerPaused;
        this.isWriteBlockEnabled = isWriteBlockEnabled;
        this.batchStartPointer = batchStartPointer;
        this.isPrimary = isPrimary;
        this.nodeName = nodeName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeOptionalString(pollerState);
        out.writeOptionalString(errorPolicy);
        out.writeBoolean(isPollerPaused);
        out.writeBoolean(isWriteBlockEnabled);
        out.writeString(batchStartPointer);

        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            // added from version 3.3 onwards
            out.writeBoolean(isPrimary);
            out.writeString(nodeName);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shardId);
        builder.field(POLLER_STATE, pollerState);
        builder.field(ERROR_POLICY, errorPolicy);
        builder.field(POLLER_PAUSED, isPollerPaused);
        builder.field(WRITE_BLOCK_ENABLED, isWriteBlockEnabled);
        builder.field(BATCH_START_POINTER, batchStartPointer);
        builder.field(IS_PRIMARY, isPrimary);
        builder.field(NODE_NAME, nodeName);
        builder.endObject();
        return builder;
    }

    /**
     * Groups list of ShardIngestionStates by the index name.
     *
     * @param shardIngestionStates the shard ingestion states
     * @return the group shard state by index
     */
    public static Map<String, List<ShardIngestionState>> groupShardStateByIndex(ShardIngestionState[] shardIngestionStates) {
        Map<String, List<ShardIngestionState>> shardIngestionStatesByIndex = new HashMap<>();

        for (ShardIngestionState state : shardIngestionStates) {
            shardIngestionStatesByIndex.computeIfAbsent(state.getIndex(), (index) -> new ArrayList<>());
            shardIngestionStatesByIndex.get(state.getIndex()).add(state);
        }

        return shardIngestionStatesByIndex;
    }

    /**
     * Returns the index.
     *
     * @return the index
     */
    public String getIndex() {
        return index;
    }

    /**
     * Returns the shard identifier.
     *
     * @return the shard identifier
     */
    public int getShardId() {
        return shardId;
    }

    /**
     * Returns the poller state.
     *
     * @return the poller state
     */
    public String getPollerState() {
        return pollerState;
    }

    /**
     * Returns the error policy.
     *
     * @return the error policy
     */
    public String getErrorPolicy() {
        return errorPolicy;
    }

    /**
     * Returns the poller paused flag.
     *
     * @return the poller paused flag
     */
    public boolean isPollerPaused() {
        return isPollerPaused;
    }

    /**
     * Returns the write block enabled flag.
     *
     * @return the write block enabled flag
     */
    public boolean isWriteBlockEnabled() {
        return isWriteBlockEnabled;
    }

    /**
     * Returns the batch start pointer.
     *
     * @return the batch start pointer
     */
    public String getBatchStartPointer() {
        return batchStartPointer;
    }

    /**
     * Returns the primary flag.
     *
     * @return the primary flag
     */
    public boolean isPrimary() {
        return isPrimary;
    }

    /**
     * Returns the node name.
     *
     * @return the node name
     */
    public String getNodeName() {
        return nodeName;
    }
}
