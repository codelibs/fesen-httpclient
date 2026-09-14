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

package org.codelibs.fesen.opensearch.action;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.support.WriteRequest;
import org.codelibs.fesen.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.codelibs.fesen.opensearch.action.support.WriteResponse;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationResponse;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.StatusToXContentObject;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;
import org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Locale;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * A base class for the response of a write operation that involves a single doc
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContentObject {

    private static final String _SHARDS = "_shards";
    private static final String _INDEX = "_index";
    private static final String _ID = "_id";
    private static final String _VERSION = "_version";
    private static final String _SEQ_NO = "_seq_no";
    private static final String _PRIMARY_TERM = "_primary_term";
    private static final String RESULT = "result";
    private static final String FORCED_REFRESH = "forced_refresh";

    /**
     * An enum that represents the results of CRUD operations, primarily used to communicate the type of
     * operation that occurred.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Result implements Writeable {
        /**
         * The CREATED value.
         */
        CREATED(0),
        /**
         * The UPDATED value.
         */
        UPDATED(1),
        /**
         * The DELETED value.
         */
        DELETED(2),
        /**
         * The NOT_FOUND value.
         */
        NOT_FOUND(3),
        /**
         * The NOOP value.
         */
        NOOP(4);

        private final byte op;
        private final String lowercase;

        Result(int op) {
            this.op = (byte) op;
            this.lowercase = this.name().toLowerCase(Locale.ROOT);
        }

        /**
         * Returns the lowercase.
         *
         * @return the lowercase
         */
        public String getLowercase() {
            return lowercase;
        }

        /**
         * Reads this instance from the given input.
         *
         * @param in the input to read from
         * @return the from
         * @throws IOException if an I/O error occurs
         */
        public static Result readFrom(StreamInput in) throws IOException {
            Byte opcode = in.readByte();
            switch (opcode) {
                case 0:
                    return CREATED;
                case 1:
                    return UPDATED;
                case 2:
                    return DELETED;
                case 3:
                    return NOT_FOUND;
                case 4:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown result code: " + opcode);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(op);
        }
    }

    private final ShardId shardId;
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private boolean forcedRefresh;
    /**
     * The result.
     */
    protected final Result result;

    /**
     * Creates a new DocWriteResponse.
     *
     * @param shardId the shard identifier
     * @param id the identifier
     * @param seqNo the seq no
     * @param primaryTerm the primary term
     * @param version the version
     * @param result the result
     */
    public DocWriteResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        this.shardId = Objects.requireNonNull(shardId);
        this.id = Objects.requireNonNull(id);
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.result = Objects.requireNonNull(result);
    }

    // needed for deserialization
    /**
     * Creates a new DocWriteResponse.
     *
     * @param shardId the shard identifier
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    protected DocWriteResponse(ShardId shardId, StreamInput in) throws IOException {
        super(in);
        this.shardId = shardId;
        if (in.getVersion().before(Version.V_2_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readString();
        version = in.readZLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        forcedRefresh = in.readBoolean();
        result = Result.readFrom(in);
    }

    /**
     * Needed for deserialization of single item requests in {@link org.codelibs.fesen.opensearch.action.index.IndexAction} and BwC
     * deserialization path
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    protected DocWriteResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readString();
        version = in.readZLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        forcedRefresh = in.readBoolean();
        result = Result.readFrom(in);
    }

    /**
     * The change that occurred to the document.
     *
     * @return the result
     */
    public Result getResult() {
        return result;
    }

    /**
     * The index the document was changed in.
     *
     * @return the index
     */
    public String getIndex() {
        return this.shardId.getIndexName();
    }

    /**
     * The id of the document changed.
     *
     * @return the identifier
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the doc.
     *
     * @return the version
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns the sequence number assigned for this change. Returns {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if the operation
     * wasn't performed (i.e., an update operation that resulted in a NOOP).
     *
     * @return the seq no
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * The primary term for this change.
     *
     * @return the primary term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    /** returns the rest status for this response (based on {@link ShardInfo#status()} */
    @Override
    public RestStatus status() {
        return getShardInfo().status();
    }

    /**
     * Writes the thin.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeWithoutShardId(out);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        writeWithoutShardId(out);
    }

    private void writeWithoutShardId(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeZLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBoolean(forcedRefresh);
        result.writeTo(out);
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
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = getShardInfo();
        builder.field(_INDEX, shardId.getIndexName());
        builder.field(_ID, id).field(_VERSION, version).field(RESULT, getResult().getLowercase());
        if (forcedRefresh) {
            builder.field(FORCED_REFRESH, true);
        }
        builder.field(_SHARDS, shardInfo);
        if (getSeqNo() >= 0) {
            builder.field(_SEQ_NO, getSeqNo());
            builder.field(_PRIMARY_TERM, getPrimaryTerm());
        }
        return builder;
    }

    /**
     * Parse the output of the {@link #innerToXContent(XContentBuilder, Params)} method.
     * <p>
     * This method is intended to be called by subclasses and must be called multiple times to parse all the information concerning
     * {@link DocWriteResponse} objects. It always parses the current token, updates the given parsing context accordingly
     * if needed and then immediately returns.
     *
     * @param parser the parser
     * @param context the context
     * @throws IOException if an I/O error occurs
     */
    protected static void parseInnerToXContent(XContentParser parser, Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        if (token.isValue()) {
            if (_INDEX.equals(currentFieldName)) {
                // index uuid and shard id are unknown and can't be parsed back for now.
                context.setShardId(new ShardId(new Index(parser.text(), IndexMetadata.INDEX_UUID_NA_VALUE), -1));
            } else if (_ID.equals(currentFieldName)) {
                context.setId(parser.text());
            } else if (_VERSION.equals(currentFieldName)) {
                context.setVersion(parser.longValue());
            } else if (RESULT.equals(currentFieldName)) {
                String result = parser.text();
                for (Result r : Result.values()) {
                    if (r.getLowercase().equals(result)) {
                        context.setResult(r);
                        break;
                    }
                }
            } else if (FORCED_REFRESH.equals(currentFieldName)) {
                context.setForcedRefresh(parser.booleanValue());
            } else if (_SEQ_NO.equals(currentFieldName)) {
                context.setSeqNo(parser.longValue());
            } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                context.setPrimaryTerm(parser.longValue());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            if (_SHARDS.equals(currentFieldName)) {
                context.setShardInfo(ShardInfo.fromXContent(parser));
            } else {
                parser.skipChildren(); // skip potential inner objects for forward compatibility
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            parser.skipChildren(); // skip potential inner arrays for forward compatibility
        }
    }

    /**
     * Base class of all {@link DocWriteResponse} builders. These {@link DocWriteResponse.Builder} are used during
     * xcontent parsing to temporarily store the parsed values, then the {@link Builder#build()} method is called to
     * instantiate the appropriate {@link DocWriteResponse} with the parsed values.
     *
     * @opensearch.internal
     */
    public abstract static class Builder {
        /**
         * Creates a new Builder.
         */
        public Builder() {
        }

        /**
         * The shard identifier.
         */
        protected ShardId shardId = null;
        /**
         * The identifier.
         */
        protected String id = null;
        /**
         * The version.
         */
        protected Long version = null;
        /**
         * The result.
         */
        protected Result result = null;
        /**
         * The forced refresh.
         */
        protected boolean forcedRefresh;
        /**
         * The shard info.
         */
        protected ShardInfo shardInfo = null;
        /**
         * The seq no.
         */
        protected long seqNo = UNASSIGNED_SEQ_NO;
        /**
         * The primary term.
         */
        protected long primaryTerm = UNASSIGNED_PRIMARY_TERM;

        /**
         * Returns the shard identifier.
         *
         * @return the shard identifier
         */
        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Sets the shard identifier.
         *
         * @param shardId the shard identifier
         */
        public void setShardId(ShardId shardId) {
            this.shardId = shardId;
        }

        /**
         * Returns the identifier.
         *
         * @return the identifier
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the identifier.
         *
         * @param id the identifier
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * Sets the version.
         *
         * @param version the version
         */
        public void setVersion(Long version) {
            this.version = version;
        }

        /**
         * Sets the result.
         *
         * @param result the result
         */
        public void setResult(Result result) {
            this.result = result;
        }

        /**
         * Sets the forced refresh.
         *
         * @param forcedRefresh the forced refresh
         */
        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }

        /**
         * Sets the shard info.
         *
         * @param shardInfo the shard info
         */
        public void setShardInfo(ShardInfo shardInfo) {
            this.shardInfo = shardInfo;
        }

        /**
         * Sets the seq no.
         *
         * @param seqNo the seq no
         */
        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
        }

        /**
         * Sets the primary term.
         *
         * @param primaryTerm the primary term
         */
        public void setPrimaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
        }

        /**
         * Builds this instance.
         *
         * @return the new instance
         */
        public abstract DocWriteResponse build();
    }
}
