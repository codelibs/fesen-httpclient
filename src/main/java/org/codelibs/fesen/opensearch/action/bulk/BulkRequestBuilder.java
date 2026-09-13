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

package org.codelibs.fesen.opensearch.action.bulk;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.action.delete.DeleteRequest;
import org.codelibs.fesen.opensearch.action.delete.DeleteRequestBuilder;
import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.index.IndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.WriteRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.replication.ReplicationRequest;
import org.codelibs.fesen.opensearch.action.update.UpdateRequest;
import org.codelibs.fesen.opensearch.action.update.UpdateRequestBuilder;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class BulkRequestBuilder extends ActionRequestBuilder<BulkRequest, BulkResponse> implements WriteRequestBuilder<BulkRequestBuilder> {

    public BulkRequestBuilder(OpenSearchClient client, BulkAction action, @Nullable String globalIndex) {
        super(client, action, new BulkRequest(globalIndex));
    }

    public BulkRequestBuilder(OpenSearchClient client, BulkAction action) {
        super(client, action, new BulkRequest());
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequestBuilder add(IndexRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(DeleteRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(DeleteRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(UpdateRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(UpdateRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }
}
