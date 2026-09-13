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

import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.WriteRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequest;
import org.codelibs.fesen.opensearch.action.support.single.instance.InstanceShardOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * Transport request builder for updating an index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpdateRequestBuilder extends InstanceShardOperationRequestBuilder<UpdateRequest, UpdateResponse, UpdateRequestBuilder>
    implements
        WriteRequestBuilder<UpdateRequestBuilder> {

    public UpdateRequestBuilder(OpenSearchClient client, UpdateAction action) {
        super(client, action, new UpdateRequest());
    }

    public UpdateRequestBuilder(OpenSearchClient client, UpdateAction action, String index, String id) {
        super(client, action, new UpdateRequest(index, id));
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     * <p>
     * The script works with the variable <code>ctx</code>, which is bound to the entry,
     * e.g. <code>ctx._source.mycounter += 1</code>.
     *
     */
    public UpdateRequestBuilder setScript(Script script) {
        request.script(script);
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public UpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        request.retryOnConflict(retryOnConflict);
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public UpdateRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.codelibs.fesen.opensearch.index.VersionType#INTERNAL}.
     */
    public UpdateRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * only perform this update request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequestBuilder setIfSeqNo(long seqNo) {
        request.setIfSeqNo(seqNo);
        return this;
    }

    /**
     * only perform this update request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequestBuilder setIfPrimaryTerm(long term) {
        request.setIfPrimaryTerm(term);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(XContentBuilder source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequestBuilder setDoc(Object... source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(Map<String, Object> source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets whether the specified doc parameter should be used as upsert document.
     */
    public UpdateRequestBuilder setDocAsUpsert(boolean shouldUpsertDoc) {
        request.docAsUpsert(shouldUpsertDoc);
        return this;
    }

    /**
     * Sets whether to perform extra effort to detect noop updates via docAsUpsert.
     * Defaults to true.
     */
    public UpdateRequestBuilder setDetectNoop(boolean detectNoop) {
        request.detectNoop(detectNoop);
        return this;
    }

    /**
     * Sets whether the script should be run in the case of an insert
     */
    public UpdateRequestBuilder setScriptedUpsert(boolean scriptedUpsert) {
        request.scriptedUpsert(scriptedUpsert);
        return this;
    }

}
