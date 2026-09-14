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

package org.codelibs.fesen.opensearch.action.support;

import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.routing.IndexRoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.IndexShardRoutingTable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;

/**
 * A class whose instances represent a value for counting the number
 * of active shard copies for a given shard in an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ActiveShardCount implements Writeable {

    private static final int ACTIVE_SHARD_COUNT_DEFAULT = -2;
    private static final int ALL_ACTIVE_SHARDS = -1;

    /**
     * The DEFAULT constant.
     */
    public static final ActiveShardCount DEFAULT = new ActiveShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    /**
     * The ALL constant.
     */
    public static final ActiveShardCount ALL = new ActiveShardCount(ALL_ACTIVE_SHARDS);
    /**
     * The NONE constant.
     */
    public static final ActiveShardCount NONE = new ActiveShardCount(0);
    /**
     * The ONE constant.
     */
    public static final ActiveShardCount ONE = new ActiveShardCount(1);

    private final int value;

    private ActiveShardCount(final int value) {
        this.value = value;
    }

    /**
     * Get an ActiveShardCount instance for the given value.  The value is first validated to ensure
     * it is a valid shard count and throws an IllegalArgumentException if validation fails.  Valid
     * values are any non-negative number.  Directly use {@link ActiveShardCount#DEFAULT} for the
     * default value (which is one shard copy) or {@link ActiveShardCount#ALL} to specify all the shards.
     *
     * @param value the value
     * @return the new instance
     */
    public static ActiveShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    /**
     * Validates that the instance is valid for the given number of replicas in an index.
     *
     * @param numberOfReplicas the number of replicas
     * @return this instance
     */
    public boolean validate(final int numberOfReplicas) {
        assert numberOfReplicas >= 0;
        return value <= numberOfReplicas + 1;
    }

    private static ActiveShardCount get(final int value) {
        switch (value) {
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return DEFAULT;
            case ALL_ACTIVE_SHARDS:
                return ALL;
            case 1:
                return ONE;
            case 0:
                return NONE;
            default:
                assert value > 1;
                return new ActiveShardCount(value);
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(value);
    }

    /**
     * Reads this instance from the given input.
     *
     * @param in the input to read from
     * @return the from
     * @throws IOException if an I/O error occurs
     */
    public static ActiveShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    /**
     * Parses the active shard count from the given string.  Valid values are "all" for
     * all shard copies, null for the default value (which defaults to one shard copy),
     * or a numeric value greater than or equal to 0. Any other input will throw an
     * IllegalArgumentException.
     *
     * @param str the str
     * @return this instance
     */
    public static ActiveShardCount parseString(final String str) {
        if (str == null) {
            return ActiveShardCount.DEFAULT;
        } else if (str.equals("all")) {
            return ActiveShardCount.ALL;
        } else {
            int val;
            try {
                val = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot parse ActiveShardCount[" + str + "]", e);
            }
            return ActiveShardCount.from(val);
        }
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ActiveShardCount that = (ActiveShardCount) o;
        return value == that.value;
    }

    @Override
    public String toString() {
        switch (value) {
            case ALL_ACTIVE_SHARDS:
                return "ALL";
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return "DEFAULT";
            default:
                return Integer.toString(value);
        }
    }

}
