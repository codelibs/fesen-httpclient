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

package org.codelibs.fesen.opensearch.action.admin.indices.forcemerge;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.common.UUIDs;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.transport.client.IndicesAdminClient;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array or {@code null} for the
 * indices.
 * {@code #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see IndicesAdminClient#forceMerge(ForceMergeRequest)
 * @see ForceMergeResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ForceMergeRequest extends BroadcastRequest<ForceMergeRequest> {

    /**
     * Defaults for the Force Merge Request
     *
     * @opensearch.internal
     */
    public static final class Defaults {
        /**
         * Creates a new Defaults.
         */
        public Defaults() {
        }

        /**
         * The MAX_NUM_SEGMENTS constant.
         */
        public static final int MAX_NUM_SEGMENTS = -1;
        /**
         * The ONLY_EXPUNGE_DELETES constant.
         */
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        /**
         * The FLUSH constant.
         */
        public static final boolean FLUSH = true;
        /**
         * The PRIMARY_ONLY constant.
         */
        public static final boolean PRIMARY_ONLY = false;
    }

    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;
    private boolean primaryOnly = Defaults.PRIMARY_ONLY;

    private static final Version FORCE_MERGE_UUID_VERSION = Version.V_3_0_0;

    /**
     * Force merge UUID to store in the live commit data of a shard under
     * the force-merge UUID commit key after force merging it.
     */
    private final String forceMergeUUID;

    private boolean shouldStoreResult;

    /**
     * Constructs a merge request over one or more indices.
     *
     * @param indices The indices to merge, no indices passed means all indices will be merged.
     */
    public ForceMergeRequest(String... indices) {
        super(indices);
        forceMergeUUID = UUIDs.randomBase64UUID();
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     *
     * @return the max num segments
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     *
     * @return the only expunge deletes
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     *
     * @return this instance
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should force merge only performed on primary shards. Defaults to {@code false}.
     *
     * @return the primary only
     */
    public boolean primaryOnly() {
        return primaryOnly;
    }

    /**
     * Should force merge only performed on primary shards. Defaults to {@code false}.
     *
     * @param primaryOnly the primary only
     * @return the primary only
     */
    public ForceMergeRequest primaryOnly(boolean primaryOnly) {
        this.primaryOnly = primaryOnly;
        return this;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public String getDescription() {
        return "Force-merge indices "
            + Arrays.toString(indices())
            + ", maxSegments["
            + maxNumSegments
            + "], onlyExpungeDeletes["
            + onlyExpungeDeletes
            + "], flush["
            + flush
            + "], primaryOnly["
            + primaryOnly
            + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            out.writeBoolean(primaryOnly);
        }
        if (out.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            out.writeString(forceMergeUUID);
        } else {
            out.writeOptionalString(forceMergeUUID);
        }
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{"
            + "maxNumSegments="
            + maxNumSegments
            + ", onlyExpungeDeletes="
            + onlyExpungeDeletes
            + ", flush="
            + flush
            + ", primaryOnly="
            + primaryOnly
            + '}';
    }
}
