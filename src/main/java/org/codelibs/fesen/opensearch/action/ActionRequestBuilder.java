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

import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Objects;

/**
 * Base Action Request Builder
 *
 * @param <Request> the request type
 * @param <Response> the response type
 * @opensearch.api
 */
public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse> {

    /**
     * The action.
     */
    protected final ActionType<Response> action;
    /**
     * The request.
     */
    protected final Request request;
    /**
     * The client.
     */
    protected final OpenSearchClient client;

    /**
     * Creates a new ActionRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param request the request
     */
    protected ActionRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.request = request;
        this.client = client;
    }

    /**
     * Returns the request.
     *
     * @return the request
     */
    public Request request() {
        return this.request;
    }

    /**
     * Executes this instance.
     *
     * @return this instance
     */
    public ActionFuture<Response> execute() {
        return client.execute(action, request);
    }

    /**
     * Short version of execute().actionGet().
     *
     * @return the value
     */
    public Response get() {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     *
     * @param timeout the timeout
     * @return the value
     */
    public Response get(TimeValue timeout) {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     *
     * @param timeout the timeout
     * @return the value
     */
    public Response get(String timeout) {
        return execute().actionGet(timeout);
    }

    /**
     * Executes this instance.
     *
     * @param listener the listener
     */
    public void execute(ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }
}
