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

package org.codelibs.fesen.opensearch.core.action.support;

import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ShardOperationFailedException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.ExceptionsHelper.detailedMessage;
import static org.codelibs.fesen.opensearch.OpenSearchException.generateThrowableXContent;
import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Exception for a default shard operation
 *
 * @opensearch.internal
 */
public class DefaultShardOperationFailedException extends ShardOperationFailedException implements Writeable {

    private static final String INDEX = "index";
    private static final String SHARD_ID = "shard";
    private static final String REASON = "reason";

    /**
     * The PARSER constant.
     */
    public static final ConstructingObjectParser<DefaultShardOperationFailedException, Void> PARSER = new ConstructingObjectParser<>(
        "failures",
        true,
        arg -> new DefaultShardOperationFailedException((String) arg[0], (int) arg[1], (Throwable) arg[2])
    );

    /**
     * Performs the declare fields step.
     *
     * @param <T> the element type
     * @param objectParser the object parser
     */
    protected static <T extends DefaultShardOperationFailedException> void declareFields(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareString(constructorArg(), new ParseField(INDEX));
        objectParser.declareInt(constructorArg(), new ParseField(SHARD_ID));
        objectParser.declareObject(constructorArg(), (p, c) -> OpenSearchException.fromXContent(p), new ParseField(REASON));
    }

    static {
        declareFields(PARSER);
    }

    /**
     * Creates a new DefaultShardOperationFailedException.
     */
    protected DefaultShardOperationFailedException() {}

    /**
     * Creates a new DefaultShardOperationFailedException by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    protected DefaultShardOperationFailedException(StreamInput in) throws IOException {
        readFrom(in, this);
    }

    /**
     * Creates a new DefaultShardOperationFailedException.
     *
     * @param index the index
     * @param shardId the shard identifier
     * @param cause the cause
     */
    public DefaultShardOperationFailedException(String index, int shardId, Throwable cause) {
        super(index, shardId, detailedMessage(cause), ExceptionsHelper.status(cause), cause);
    }

    /**
     * Reads the shard operation failed.
     *
     * @param in the input to read from
     * @return the shard operation failed
     * @throws IOException if an I/O error occurs
     */
    public static DefaultShardOperationFailedException readShardOperationFailed(StreamInput in) throws IOException {
        return new DefaultShardOperationFailedException(in);
    }

    /**
     * Reads this instance from the given input.
     *
     * @param in the input to read from
     * @param f the f
     * @throws IOException if an I/O error occurs
     */
    public static void readFrom(StreamInput in, DefaultShardOperationFailedException f) throws IOException {
        f.index = in.readOptionalString();
        f.shardId = in.readVInt();
        f.cause = in.readException();
        f.status = RestStatus.readFrom(in);
        f.reason = detailedMessage(f.cause);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeVInt(shardId);
        out.writeException(cause);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "[" + index + "][" + shardId + "] failed, reason [" + reason() + "]";
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the inner to XContent.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @return the inner to XContent
     * @throws IOException if an I/O error occurs
     */
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("shard", shardId());
        builder.field("index", index());
        builder.field("status", status.name());
        if (reason != null) {
            builder.startObject("reason");
            generateThrowableXContent(builder, params, cause);
            builder.endObject();
        }
        return builder;
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     */
    public static DefaultShardOperationFailedException fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
