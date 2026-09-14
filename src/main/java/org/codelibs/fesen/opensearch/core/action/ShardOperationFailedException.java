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

package org.codelibs.fesen.opensearch.core.action;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;

import java.util.Objects;

/**
 * An exception indicating that a failure occurred performing an operation on the shard.
 *
 * @opensearch.internal
 */
public abstract class ShardOperationFailedException implements Writeable, ToXContentObject {

    /**
     * The index.
     */
    protected String index;
    /**
     * The shard identifier.
     */
    protected int shardId = -1;
    /**
     * The reason.
     */
    protected String reason;
    /**
     * The status.
     */
    protected RestStatus status;
    /**
     * The cause.
     */
    protected Throwable cause;

    /**
     * Creates a new ShardOperationFailedException.
     */
    protected ShardOperationFailedException() {

    }

    /**
     * Creates a new ShardOperationFailedException.
     *
     * @param index the index
     * @param shardId the shard identifier
     * @param reason the reason
     * @param status the status
     * @param cause the cause
     */
    protected ShardOperationFailedException(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
        this.index = index;
        this.shardId = shardId;
        this.reason = Objects.requireNonNull(reason, "reason cannot be null");
        this.status = Objects.requireNonNull(status, "status cannot be null");
        this.cause = Objects.requireNonNull(cause, "cause cannot be null");
    }

    /**
     * The index the operation failed on. Might return {@code null} if it can't be derived.
     *
     * @return this instance
     */
    @Nullable
    public final String index() {
        return index;
    }

    /**
     * The index the operation failed on. Might return {@code -1} if it can't be derived.
     *
     * @return the shard identifier
     */
    public final int shardId() {
        return shardId;
    }

    /**
     * The reason of the failure.
     *
     * @return the reason
     */
    public final String reason() {
        return reason;
    }

    /**
     * The status of the failure.
     *
     * @return the status
     */
    public final RestStatus status() {
        return status;
    }

    /**
     * The cause of this failure
     *
     * @return the cause
     */
    public final Throwable getCause() {
        return cause;
    }
}
