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

package org.codelibs.fesen.opensearch.action.index;

import org.codelibs.fesen.opensearch.action.DocWriteRequest;
import org.codelibs.fesen.opensearch.action.support.WriteRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequestBuilder;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * An index document action request builder.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder>
    implements
        WriteRequestBuilder<IndexRequestBuilder> {

    /**
     * Creates a new IndexRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public IndexRequestBuilder(OpenSearchClient client, IndexAction action) {
        super(client, action, new IndexRequest());
    }

    /**
     * Creates a new IndexRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param index the index
     */
    public IndexRequestBuilder(OpenSearchClient client, IndexAction action, @Nullable String index) {
        super(client, action, new IndexRequest(index));
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     *
     * @param id the identifier
     * @return this instance
     */
    public IndexRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     * @return this instance
     */
    public IndexRequestBuilder setSource(Map<String, ?> source) {
        request.source(source);
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     * @param contentType the content type
     * @return this instance
     */
    public IndexRequestBuilder setSource(Map<String, ?> source, MediaType contentType) {
        request.source(source, contentType);
        return this;
    }

    /**
     * Sets the document source to index.
     * <p>
     * Note, its preferable to either set it using {@link #setSource(XContentBuilder)}
     * or using the {@code #setSource(byte[], MediaType)}.
     *
     * @param source the source
     * @param mediaType the media type
     * @return this instance
     */
    public IndexRequestBuilder setSource(String source, MediaType mediaType) {
        request.source(source, mediaType);
        return this;
    }

    /**
     * Sets the content source to index.
     *
     * @param sourceBuilder the source builder
     * @return this instance
     */
    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    /**
     * Constructs a simple document with a field name and value pairs.
     * <p>
     * <b>Note: the number of objects passed to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     *
     * @param source the source
     * @return this instance
     */
    public IndexRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     *
     * @param opType the op type
     * @return this instance
     */
    public IndexRequestBuilder setOpType(DocWriteRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    /**
     * Set to {@code true} to force this index to use {@link org.codelibs.fesen.opensearch.action.index.IndexRequest.OpType#CREATE}.
     *
     * @param create the create
     * @return this instance
     */
    public IndexRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * only perform this indexing request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     *
     * @param seqNo the seq no
     * @return this instance
     */
    public IndexRequestBuilder setIfSeqNo(long seqNo) {
        request.setIfSeqNo(seqNo);
        return this;
    }

    /**
     * only perform this indexing request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     *
     * @param term the term
     * @return this instance
     */
    public IndexRequestBuilder setIfPrimaryTerm(long term) {
        request.setIfPrimaryTerm(term);
        return this;
    }

    /**
     * Sets the ingest pipeline to be executed before indexing the document
     *
     * @param pipeline the pipeline
     * @return this instance
     */
    public IndexRequestBuilder setPipeline(String pipeline) {
        request.setPipeline(pipeline);
        return this;
    }
}
