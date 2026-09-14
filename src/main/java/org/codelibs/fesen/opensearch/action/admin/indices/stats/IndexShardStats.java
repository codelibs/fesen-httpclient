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

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * IndexShardStats for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexShardStats implements Iterable<ShardStats>, Writeable {

    private final ShardId shardId;

    private final ShardStats[] shards;

    /**
     * Creates a new IndexShardStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public IndexShardStats(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        shards = in.readArray(ShardStats::new, ShardStats[]::new);
    }

    /**
     * Creates a new IndexShardStats.
     *
     * @param shardId the shard identifier
     * @param shards the shards
     */
    public IndexShardStats(ShardId shardId, ShardStats[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    /**
     * Returns the shard identifier.
     *
     * @return the shard identifier
     */
    public ShardId getShardId() {
        return this.shardId;
    }

    /**
     * Returns the shards.
     *
     * @return the shards
     */
    public ShardStats[] getShards() {
        return shards;
    }

    @Override
    public Iterator<ShardStats> iterator() {
        return Arrays.stream(shards).iterator();
    }

    private CommonStats total = null;

    private CommonStats primary = null;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeArray(shards);
    }
}
