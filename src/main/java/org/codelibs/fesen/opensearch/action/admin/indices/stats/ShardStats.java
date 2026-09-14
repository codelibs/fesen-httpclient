/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.stats;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.engine.CommitStats;
import org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseStats;
import org.codelibs.fesen.opensearch.index.seqno.SeqNoStats;
import org.codelibs.fesen.opensearch.indices.pollingingest.PollingIngestStats;

import java.io.IOException;

/**
 * Shard Stats for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardStats implements Writeable, ToXContentFragment {

    private ShardRouting shardRouting;
    private CommonStats commonStats;
    @Nullable
    private CommitStats commitStats;
    @Nullable
    private SeqNoStats seqNoStats;

    @Nullable
    private RetentionLeaseStats retentionLeaseStats;

    @Nullable
    private PollingIngestStats pollingIngestStats;

    private String dataPath;
    private String statePath;
    private boolean isCustomDataPath;

    /**
     * Creates a new ShardStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ShardStats(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        commonStats = new CommonStats(in);
        commitStats = CommitStats.readOptionalCommitStatsFrom(in);
        statePath = in.readString();
        dataPath = in.readString();
        isCustomDataPath = in.readBoolean();
        seqNoStats = in.readOptionalWriteable(SeqNoStats::new);
        retentionLeaseStats = in.readOptionalWriteable(RetentionLeaseStats::new);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            pollingIngestStats = in.readOptionalWriteable(PollingIngestStats::new);
        }
    }

    /**
     * The shard routing information (cluster wide shard state).
     *
     * @return the shard routing
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    /**
     * Returns the stats.
     *
     * @return the stats
     */
    public CommonStats getStats() {
        return this.commonStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        commonStats.writeTo(out);
        out.writeOptionalWriteable(commitStats);
        out.writeString(statePath);
        out.writeString(dataPath);
        out.writeBoolean(isCustomDataPath);
        out.writeOptionalWriteable(seqNoStats);
        out.writeOptionalWriteable(retentionLeaseStats);
        if (out.getVersion().onOrAfter((Version.V_3_0_0))) {
            out.writeOptionalWriteable(pollingIngestStats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ROUTING)
            .field(Fields.STATE, shardRouting.state())
            .field(Fields.PRIMARY, shardRouting.primary())
            .field(Fields.NODE, shardRouting.currentNodeId())
            .field(Fields.RELOCATING_NODE, shardRouting.relocatingNodeId())
            .endObject();

        commonStats.toXContent(builder, params);
        if (commitStats != null) {
            commitStats.toXContent(builder, params);
        }
        if (seqNoStats != null) {
            seqNoStats.toXContent(builder, params);
        }
        if (retentionLeaseStats != null) {
            retentionLeaseStats.toXContent(builder, params);
        }
        if (pollingIngestStats != null) {
            pollingIngestStats.toXContent(builder, params);
        }
        builder.startObject(Fields.SHARD_PATH);
        builder.field(Fields.STATE_PATH, statePath);
        builder.field(Fields.DATA_PATH, dataPath);
        builder.field(Fields.IS_CUSTOM_DATA_PATH, isCustomDataPath);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String STATE_PATH = "state_path";
        static final String DATA_PATH = "data_path";
        static final String IS_CUSTOM_DATA_PATH = "is_custom_data_path";
        static final String SHARD_PATH = "shard_path";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";
    }

}
