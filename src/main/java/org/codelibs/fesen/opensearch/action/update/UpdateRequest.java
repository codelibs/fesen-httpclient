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

package org.codelibs.fesen.opensearch.action.update;

import org.apache.lucene.util.RamUsageEstimator;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.DocWriteRequest;
import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.WriteRequest;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequest;
import org.codelibs.fesen.opensearch.action.support.single.instance.InstanceShardOperationRequest;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.lucene.uid.Versions;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.script.ScriptType;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Transport request for updating an index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpdateRequest extends InstanceShardOperationRequest<UpdateRequest>
    implements
        DocWriteRequest<UpdateRequest>,
        WriteRequest<UpdateRequest>,
        ToXContentObject {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(UpdateRequest.class);

    private static ObjectParser<UpdateRequest, Void> PARSER;

    private static final ParseField SCRIPT_FIELD = new ParseField("script");
    private static final ParseField SCRIPTED_UPSERT_FIELD = new ParseField("scripted_upsert");
    private static final ParseField UPSERT_FIELD = new ParseField("upsert");
    private static final ParseField DOC_FIELD = new ParseField("doc");
    private static final ParseField DOC_AS_UPSERT_FIELD = new ParseField("doc_as_upsert");
    private static final ParseField DETECT_NOOP_FIELD = new ParseField("detect_noop");
    private static final ParseField SOURCE_FIELD = new ParseField("_source");
    private static final ParseField IF_SEQ_NO = new ParseField("if_seq_no");
    private static final ParseField IF_PRIMARY_TERM = new ParseField("if_primary_term");

    static {
        PARSER = new ObjectParser<>(UpdateRequest.class.getSimpleName());
        PARSER.declareField(
            (request, script) -> request.script = script,
            (parser, context) -> Script.parse(parser),
            SCRIPT_FIELD,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareBoolean(UpdateRequest::scriptedUpsert, SCRIPTED_UPSERT_FIELD);
        PARSER.declareObject((request, builder) -> request.safeUpsertRequest().source(builder), (parser, context) -> {
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(parser.contentType());
            builder.copyCurrentStructure(parser);
            return builder;
        }, UPSERT_FIELD);
        PARSER.declareObject((request, builder) -> request.safeDoc().source(builder), (parser, context) -> {
            XContentBuilder docBuilder = MediaTypeRegistry.contentBuilder(parser.contentType());
            docBuilder.copyCurrentStructure(parser);
            return docBuilder;
        }, DOC_FIELD);
        PARSER.declareBoolean(UpdateRequest::docAsUpsert, DOC_AS_UPSERT_FIELD);
        PARSER.declareBoolean(UpdateRequest::detectNoop, DETECT_NOOP_FIELD);
        PARSER.declareField(
            UpdateRequest::fetchSource,
            (parser, context) -> FetchSourceContext.fromXContent(parser),
            SOURCE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING
        );
        PARSER.declareLong(UpdateRequest::setIfSeqNo, IF_SEQ_NO);
        PARSER.declareLong(UpdateRequest::setIfPrimaryTerm, IF_PRIMARY_TERM);
    }

    private String id;
    @Nullable
    private String routing;

    @Nullable
    Script script;

    private FetchSourceContext fetchSourceContext;

    private int retryOnConflict = 0;
    private long ifSeqNo = UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private IndexRequest upsertRequest;

    private boolean scriptedUpsert = false;
    private boolean docAsUpsert = false;
    private boolean detectNoop = true;
    private boolean requireAlias = false;

    @Nullable
    private IndexRequest doc;

    /**
     * Creates a new UpdateRequest.
     */
    public UpdateRequest() {}

    /**
     * Creates a new UpdateRequest.
     *
     * @param shardId the shard identifier
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public UpdateRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readString();
        routing = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        retryOnConflict = in.readVInt();
        refreshPolicy = RefreshPolicy.readFrom(in);
        if (in.readBoolean()) {
            doc = new IndexRequest(shardId, in);
        }
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            upsertRequest = new IndexRequest(shardId, in);
        }
        docAsUpsert = in.readBoolean();
        ifSeqNo = in.readZLong();
        ifPrimaryTerm = in.readVLong();
        detectNoop = in.readBoolean();
        scriptedUpsert = in.readBoolean();
        requireAlias = in.readBoolean();
    }

    /**
     * Creates a new UpdateRequest.
     *
     * @param index the index
     * @param id the identifier
     */
    public UpdateRequest(String index, String id) {
        super(index);
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (upsertRequest != null && upsertRequest.version() != Versions.MATCH_ANY) {
            validationException = addValidationError("can't provide version in upsert request", validationException);
        }
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }

        validationException = DocWriteRequest.validateSeqNoBasedCASParams(this, validationException);

        if (ifSeqNo != UNASSIGNED_SEQ_NO) {
            if (retryOnConflict > 0) {
                validationException = addValidationError("compare and write operations can not be retried", validationException);
            }

            if (docAsUpsert) {
                validationException = addValidationError("compare and write operations can not be used with upsert", validationException);
            }
            if (upsertRequest != null) {
                validationException = addValidationError(
                    "upsert requests don't support `if_seq_no` and `if_primary_term`",
                    validationException
                );
            }
        }

        if (script == null && doc == null) {
            validationException = addValidationError("script or doc is missing", validationException);
        }
        if (script != null && doc != null) {
            validationException = addValidationError("can't provide both script and doc", validationException);
        }
        if (doc == null && docAsUpsert) {
            validationException = addValidationError("doc must be specified if doc_as_upsert is enabled", validationException);
        }

        validationException = DocWriteRequest.validateDocIdLength(id, validationException);

        return validationException;
    }

    /**
     * The id of the indexed document.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document.
     *
     * @param id the identifier
     * @return the identifier
     */
    public UpdateRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public UpdateRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    /**
     * Returns the script.
     *
     * @return the script
     */
    public Script script() {
        return this.script;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     *
     * @param script the script
     * @return the script
     */
    public UpdateRequest script(Script script) {
        this.script = script;
        return this;
    }

    /**
     * Explicitly set the fetch source context for this request
     *
     * @param context the context
     * @return this instance
     */
    public UpdateRequest fetchSource(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     *
     * @param retryOnConflict the retry on conflict
     * @return this instance
     */
    public UpdateRequest retryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    /**
     * Retries the on conflict.
     *
     * @return this instance
     */
    public int retryOnConflict() {
        return this.retryOnConflict;
    }

    @Override
    public UpdateRequest version(long version) {
        throw new UnsupportedOperationException("update requests do not support versioning");
    }

    @Override
    public long version() {
        return Versions.MATCH_ANY;
    }

    @Override
    public UpdateRequest versionType(VersionType versionType) {
        throw new UnsupportedOperationException("update requests do not support versioning");
    }

    @Override
    public VersionType versionType() {
        return VersionType.INTERNAL;
    }

    /**
     * only perform this update request if the document's modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequest setIfSeqNo(long seqNo) {
        if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" + seqNo + "].");
        }
        ifSeqNo = seqNo;
        return this;
    }

    /**
     * only performs this update request if the document's last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequest setIfPrimaryTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        ifPrimaryTerm = term;
        return this;
    }

    /**
     * If set, only perform this update request if the document was last modification was assigned this sequence number.
     * If the document last modification was assigned a different sequence number a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * If set, only perform this update request if the document was last modification was assigned this primary term.
     * <p>
     * If the document last modification was assigned a different term a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    @Override
    public OpType opType() {
        return OpType.UPDATE;
    }

    @Override
    public UpdateRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * Waits the for active shards.
     *
     * @return this instance
     */
    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     *
     * @param source the source
     * @return the doc
     */
    public UpdateRequest doc(XContentBuilder source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     *
     * @param source the source
     * @return the doc
     */
    public UpdateRequest doc(Object... source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Returns the doc.
     *
     * @return the doc
     */
    public IndexRequest doc() {
        return this.doc;
    }

    private IndexRequest safeDoc() {
        if (doc == null) {
            doc = new IndexRequest(index);
        }
        return doc;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     *
     * @param source the source
     * @return the upsert
     */
    public UpdateRequest upsert(Map<String, Object> source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Returns the upsert request.
     *
     * @return the upsert request
     */
    public IndexRequest upsertRequest() {
        return this.upsertRequest;
    }

    private IndexRequest safeUpsertRequest() {
        if (upsertRequest == null) {
            upsertRequest = new IndexRequest(index);
        }
        return upsertRequest;
    }

    /**
     * Should this update attempt to detect if it is a noop? Defaults to true.
     * @param detectNoop the detect noop
     * @return this for chaining
     */
    public UpdateRequest detectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
        return this;
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public UpdateRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, this, null);
    }

    /**
     * Returns the doc as upsert.
     *
     * @return the doc as upsert
     */
    public boolean docAsUpsert() {
        return this.docAsUpsert;
    }

    /**
     * Returns the doc as upsert.
     *
     * @param shouldUpsertDoc the should upsert doc
     * @return the doc as upsert
     */
    public UpdateRequest docAsUpsert(boolean shouldUpsertDoc) {
        this.docAsUpsert = shouldUpsertDoc;
        return this;
    }

    /**
     * Returns the scripted upsert.
     *
     * @param scriptedUpsert the scripted upsert
     * @return the scripted upsert
     */
    public UpdateRequest scriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
        return this;
    }

    @Override
    public boolean isRequireAlias() {
        return requireAlias;
    }

    /**
     * Sets the require alias.
     *
     * @param requireAlias the require alias
     * @return this instance
     */
    public UpdateRequest setRequireAlias(boolean requireAlias) {
        this.requireAlias = requireAlias;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        doWrite(out, false);
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        doWrite(out, true);
    }

    private void doWrite(StreamOutput out, boolean thin) throws IOException {
        waitForActiveShards.writeTo(out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeOptionalString(routing);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        out.writeVInt(retryOnConflict);
        refreshPolicy.writeTo(out);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            doc.index(index);
            doc.id(id);
            if (thin) {
                doc.writeThin(out);
            } else {
                doc.writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            upsertRequest.index(index);
            upsertRequest.id(id);
            if (thin) {
                upsertRequest.writeThin(out);
            } else {
                upsertRequest.writeTo(out);
            }
        }
        out.writeBoolean(docAsUpsert);
        out.writeZLong(ifSeqNo);
        out.writeVLong(ifPrimaryTerm);
        out.writeBoolean(detectNoop);
        out.writeBoolean(scriptedUpsert);
        out.writeBoolean(requireAlias);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (docAsUpsert) {
            builder.field("doc_as_upsert", docAsUpsert);
        }
        if (doc != null) {
            MediaType mediaType = doc.getContentType();
            try (
                XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    doc.source(),
                    mediaType
                )
            ) {
                builder.field("doc");
                builder.copyCurrentStructure(parser);
            }
        }

        if (ifSeqNo != UNASSIGNED_SEQ_NO) {
            builder.field(IF_SEQ_NO.getPreferredName(), ifSeqNo);
            builder.field(IF_PRIMARY_TERM.getPreferredName(), ifPrimaryTerm);
        }

        if (script != null) {
            builder.field("script", script);
        }
        if (upsertRequest != null) {
            MediaType mediaType = upsertRequest.getContentType();
            try (
                XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    upsertRequest.source(),
                    mediaType
                )
            ) {
                builder.field("upsert");
                builder.copyCurrentStructure(parser);
            }
        }
        if (scriptedUpsert) {
            builder.field("scripted_upsert", scriptedUpsert);
        }
        if (detectNoop == false) {
            builder.field("detect_noop", detectNoop);
        }
        if (fetchSourceContext != null) {
            builder.field("_source", fetchSourceContext);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder().append("update {[").append(index).append("][").append(id).append("]");
        res.append(", doc_as_upsert[").append(docAsUpsert).append("]");
        if (doc != null) {
            res.append(", doc[").append(doc).append("]");
        }
        if (script != null) {
            res.append(", script[").append(script).append("]");
        }
        if (upsertRequest != null) {
            res.append(", upsert[").append(upsertRequest).append("]");
        }
        res.append(", scripted_upsert[").append(scriptedUpsert).append("]");
        res.append(", detect_noop[").append(detectNoop).append("]");
        return res.append("}").toString();
    }

    @Override
    public long ramBytesUsed() {
        long childRequestBytes = 0;
        if (doc != null) {
            childRequestBytes += doc.ramBytesUsed();
        }
        if (upsertRequest != null) {
            childRequestBytes += upsertRequest.ramBytesUsed();
        }
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(id) + childRequestBytes;
    }
}
