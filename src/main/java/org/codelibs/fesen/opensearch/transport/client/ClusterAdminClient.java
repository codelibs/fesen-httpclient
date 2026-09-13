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

package org.codelibs.fesen.opensearch.transport.client;

import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ClusterAdminClient extends OpenSearchClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     * @see Requests#clusterHealthRequest(String...)
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterHealthRequest(String...)
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     * @see Requests#nodesStatsRequest(String...)
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request.
     *
     */
    ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids specified in the request.
     */
    void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener);

    /**
     * Returns a request builder to fetch top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids provided. Note: Use {@code *} to fetch samples for all nodes
     */
    NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds);

}
