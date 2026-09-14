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

package org.codelibs.fesen.opensearch.action.support.clustermanager;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Base request builder for cluster-manager node operations
 *
 * @param <Request> the request type
 * @param <Response> the response type
 * @param <RequestBuilder> the request builder type
 * @opensearch.internal
 */
public abstract class ClusterManagerNodeOperationRequestBuilder<
    Request extends ClusterManagerNodeRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends ClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestBuilder<
        Request,
        Response> {

    /**
     * Creates a new ClusterManagerNodeOperationRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param request the request
     */
    protected ClusterManagerNodeOperationRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
     *
     * @param timeout the timeout
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setClusterManagerNodeTimeout(TimeValue timeout) {
        request.clusterManagerNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
     *
     * @param timeout the timeout
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setClusterManagerNodeTimeout(String timeout) {
        request.clusterManagerNodeTimeout(timeout);
        return (RequestBuilder) this;
    }
}
