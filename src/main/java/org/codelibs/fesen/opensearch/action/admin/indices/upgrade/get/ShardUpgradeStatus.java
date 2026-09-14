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

package org.codelibs.fesen.opensearch.action.admin.indices.upgrade.get;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastShardResponse;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Status for a Shard Upgrade
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardUpgradeStatus extends BroadcastShardResponse {

    private ShardRouting shardRouting;

    private long totalBytes;

    private long toUpgradeBytes;

    private long toUpgradeBytesAncient;

    /**
     * Creates a new ShardUpgradeStatus by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ShardUpgradeStatus(StreamInput in) throws IOException {
        super(in);
        shardRouting = new ShardRouting(in);
        totalBytes = in.readLong();
        toUpgradeBytes = in.readLong();
        toUpgradeBytesAncient = in.readLong();
    }

    /**
     * Returns the shard routing.
     *
     * @return the shard routing
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    /**
     * Returns the total bytes.
     *
     * @return the total bytes
     */
    public long getTotalBytes() {
        return totalBytes;
    }

    /**
     * Returns the to upgrade bytes.
     *
     * @return the to upgrade bytes
     */
    public long getToUpgradeBytes() {
        return toUpgradeBytes;
    }

    /**
     * Returns the to upgrade bytes ancient.
     *
     * @return the to upgrade bytes ancient
     */
    public long getToUpgradeBytesAncient() {
        return toUpgradeBytesAncient;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeLong(totalBytes);
        out.writeLong(toUpgradeBytes);
        out.writeLong(toUpgradeBytesAncient);
    }
}
