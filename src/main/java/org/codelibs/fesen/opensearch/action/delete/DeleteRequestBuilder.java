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

package org.codelibs.fesen.opensearch.action.delete;

import org.codelibs.fesen.opensearch.action.support.WriteRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequestBuilder;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * A delete document action request builder.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DeleteRequestBuilder extends ReplicationRequestBuilder<DeleteRequest, DeleteResponse, DeleteRequestBuilder>
    implements
        WriteRequestBuilder<DeleteRequestBuilder> {

    public DeleteRequestBuilder(OpenSearchClient client, DeleteAction action, @Nullable String index) {
        super(client, action, new DeleteRequest(index));
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * only perform this delete request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public DeleteRequestBuilder setIfSeqNo(long seqNo) {
        request.setIfSeqNo(seqNo);
        return this;
    }

    /**
     * only perform this delete request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.codelibs.fesen.opensearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public DeleteRequestBuilder setIfPrimaryTerm(long term) {
        request.setIfPrimaryTerm(term);
        return this;
    }

}
