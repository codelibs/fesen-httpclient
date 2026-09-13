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

package org.codelibs.fesen.opensearch.action.support.replication;

import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Requests that are run on a particular replica, first on the primary and then on the replicas like {@link IndexRequest} or
 * the shard refresh action.
 *
 * @opensearch.internal
 */
public abstract class ReplicationRequest<Request extends ReplicationRequest<Request>> extends ActionRequest implements IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    /**
     * Target shard the request should execute on. In case of index and delete requests,
     * shard id gets resolved by the transport action before performing request operation
     * and at request creation time for shard-level bulk, refresh and flush requests.
     */
    protected final ShardId shardId;

    protected TimeValue timeout;
    protected String index;

    /**
     * The number of shard copies that must be active before proceeding with the replication action.
     */
    protected ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private long routedBasedOnClusterVersion = 0;

    public ReplicationRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(in);
        final boolean thinRead = shardId != null;
        if (thinRead) {
            this.shardId = shardId;
        } else {
            this.shardId = in.readOptionalWriteable(ShardId::new);
        }
        waitForActiveShards = ActiveShardCount.readFrom(in);
        timeout = in.readTimeValue();
        if (thinRead) {
            if (in.readBoolean()) {
                index = in.readString();
            } else {
                index = shardId.getIndexName();
            }
        } else {
            index = in.readString();
        }
        routedBasedOnClusterVersion = in.readVLong();
    }

    /**
     * Creates a new request with resolved shard id
     */
    public ReplicationRequest(@Nullable ShardId shardId) {
        this.index = shardId == null ? null : shardId.getIndexName();
        this.shardId = shardId;
        this.timeout = DEFAULT_TIMEOUT;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final Request index(String index) {
        this.index = index;
        return (Request) this;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        waitForActiveShards.writeTo(out);
        out.writeTimeValue(timeout);
        out.writeString(index);
        out.writeVLong(routedBasedOnClusterVersion);
    }

    /**
     * Thin serialization that does not write {@link #shardId} and will only write {@link #index} if it is different from the index name in
     * {@link #shardId}.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeTimeValue(timeout);
        if (shardId != null && index.equals(shardId.getIndexName())) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(index);
        }
        out.writeVLong(routedBasedOnClusterVersion);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ReplicationTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public abstract String toString(); // force a proper to string to ease debugging

    @Override
    public String getDescription() {
        return toString();
    }

    /**
     * This method is called before this replication request is retried
     * the first time.
     */
    public void onRetry() {
        // nothing by default
    }
}
