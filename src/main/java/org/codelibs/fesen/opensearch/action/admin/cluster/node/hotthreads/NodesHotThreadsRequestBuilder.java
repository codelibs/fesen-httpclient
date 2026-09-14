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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads;

import org.codelibs.fesen.opensearch.action.support.nodes.NodesOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Builder class for requesting OpenSearch Hot Threads
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesHotThreadsRequestBuilder extends NodesOperationRequestBuilder<
    NodesHotThreadsRequest,
    NodesHotThreadsResponse,
    NodesHotThreadsRequestBuilder> {

    /**
     * Creates a new NodesHotThreadsRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public NodesHotThreadsRequestBuilder(OpenSearchClient client, NodesHotThreadsAction action) {
        super(client, action, new NodesHotThreadsRequest());
    }

    /**
     * Sets the threads.
     *
     * @param threads the threads
     * @return this instance
     */
    public NodesHotThreadsRequestBuilder setThreads(int threads) {
        request.threads(threads);
        return this;
    }

    /**
     * Sets the ignore idle threads.
     *
     * @param ignoreIdleThreads the ignore idle threads
     * @return this instance
     */
    public NodesHotThreadsRequestBuilder setIgnoreIdleThreads(boolean ignoreIdleThreads) {
        request.ignoreIdleThreads(ignoreIdleThreads);
        return this;
    }

    /**
     * Sets the type.
     *
     * @param type the type
     * @return this instance
     */
    public NodesHotThreadsRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the interval.
     *
     * @param interval the interval
     * @return this instance
     */
    public NodesHotThreadsRequestBuilder setInterval(TimeValue interval) {
        request.interval(interval);
        return this;
    }
}
