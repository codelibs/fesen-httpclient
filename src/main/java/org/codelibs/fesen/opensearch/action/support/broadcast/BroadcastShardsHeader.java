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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.action.support.broadcast;

import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ShardOperationFailedException;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent.Params;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Renders and names the {@code _shards} header shared by every broadcast-style response.
 *
 * <p>This is the client-side remnant of {@code org.opensearch.rest.action.RestActions}: only the
 * field names and the {@code _shards} renderer are needed to serialise and parse a response, so
 * the REST-layer helpers that surrounded them are not carried over.</p>
 *
 * @opensearch.internal
 */
public class BroadcastShardsHeader {

    /** The {@code _shards} object holding the per-request shard counters. */
    public static final ParseField _SHARDS_FIELD = new ParseField("_shards");
    /** The total number of shards the request targeted. */
    public static final ParseField TOTAL_FIELD = new ParseField("total");
    /** The number of shards the request succeeded on. */
    public static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    /** The number of shards the request skipped. */
    public static final ParseField SKIPPED_FIELD = new ParseField("skipped");
    /** The number of shards the request failed on. */
    public static final ParseField FAILED_FIELD = new ParseField("failed");
    /** The per-shard failures. */
    public static final ParseField FAILURES_FIELD = new ParseField("failures");

    private BroadcastShardsHeader() {
    }

    /**
     * Renders the {@code _shards} header for a broadcast response.
     *
     * @param builder the builder to render into
     * @param params the rendering parameters
     * @param response the response to take the shard counters from
     * @throws IOException if rendering fails
     */
    public static void buildBroadcastShardsHeader(XContentBuilder builder, Params params, BroadcastResponse response)
        throws IOException {
        buildBroadcastShardsHeader(
            builder,
            params,
            response.getTotalShards(),
            response.getSuccessfulShards(),
            -1,
            response.getFailedShards(),
            response.getShardFailures()
        );
    }

    /**
     * Renders the {@code _shards} header from explicit shard counters.
     *
     * @param builder the builder to render into
     * @param params the rendering parameters
     * @param total the total number of shards
     * @param successful the number of successful shards
     * @param skipped the number of skipped shards, or a negative value to omit the field
     * @param failed the number of failed shards
     * @param shardFailures the per-shard failures, possibly empty
     * @throws IOException if rendering fails
     */
    public static void buildBroadcastShardsHeader(
        XContentBuilder builder,
        Params params,
        int total,
        int successful,
        int skipped,
        int failed,
        ShardOperationFailedException[] shardFailures
    ) throws IOException {
        builder.startObject(_SHARDS_FIELD.getPreferredName());
        builder.field(TOTAL_FIELD.getPreferredName(), total);
        builder.field(SUCCESSFUL_FIELD.getPreferredName(), successful);
        if (skipped >= 0) {
            builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
        }
        builder.field(FAILED_FIELD.getPreferredName(), failed);
        if (CollectionUtils.isEmpty(shardFailures) == false) {
            builder.startArray(FAILURES_FIELD.getPreferredName());
            for (ShardOperationFailedException shardFailure : ExceptionsHelper.groupBy(shardFailures)) {
                shardFailure.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
    }
}
