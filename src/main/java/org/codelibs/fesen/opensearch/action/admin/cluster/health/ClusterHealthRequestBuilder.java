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

package org.codelibs.fesen.opensearch.action.admin.cluster.health;

import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.codelibs.fesen.opensearch.cluster.health.ClusterHealthStatus;
import org.codelibs.fesen.opensearch.common.Priority;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for requesting cluster health
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterHealthRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterHealthRequest,
    ClusterHealthResponse,
    ClusterHealthRequestBuilder> {

    public ClusterHealthRequestBuilder(OpenSearchClient client, ClusterHealthAction action) {
        super(client, action, new ClusterHealthRequest());
    }

    public ClusterHealthRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ClusterHealthRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForStatus(ClusterHealthStatus waitForStatus) {
        request.waitForStatus(waitForStatus);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForGreenStatus() {
        request.waitForGreenStatus();
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForYellowStatus() {
        request.waitForYellowStatus();
        return this;
    }

    /**
     * Sets whether the request should wait for there to be no relocating shards before
     * retrieving the cluster health status.  Defaults to <code>false</code>, meaning the
     * operation does not wait on there being no more relocating shards.  Set to <code>true</code>
     * to wait until the number of relocating shards in the cluster is 0.
     */
    public ClusterHealthRequestBuilder setWaitForNoRelocatingShards(boolean waitForRelocatingShards) {
        request.waitForNoRelocatingShards(waitForRelocatingShards);
        return this;
    }

    /**
     * Sets whether the request should wait for there to be no initializing shards before
     * retrieving the cluster health status.  Defaults to <code>false</code>, meaning the
     * operation does not wait on there being no more initializing shards.  Set to <code>true</code>
     * to wait until the number of initializing shards in the cluster is 0.
     */
    public ClusterHealthRequestBuilder setWaitForNoInitializingShards(boolean waitForNoInitializingShards) {
        request.waitForNoInitializingShards(waitForNoInitializingShards);
        return this;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, "&gt;12" and "&lt;12" for range.
     */
    public ClusterHealthRequestBuilder setWaitForNodes(String waitForNodes) {
        request.waitForNodes(waitForNodes);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForEvents(Priority waitForEvents) {
        request.waitForEvents(waitForEvents);
        return this;
    }

    public ClusterHealthRequestBuilder setAwarenessAttribute(String awarenessAttribute) {
        request.setAwarenessAttribute(awarenessAttribute);
        return this;
    }

    /**
     * Specifies if the local request should ensure that the local node is commissioned
     */
    public final ClusterHealthRequestBuilder setEnsureNodeWeighedIn(boolean ensureNodeCommissioned) {
        request.ensureNodeWeighedIn(ensureNodeCommissioned);
        return this;
    }

    public ClusterHealthRequestBuilder setApplyLevelAtTransportLayer(boolean applyLevelAtTransportLayer) {
        request.setApplyLevelAtTransportLayer(applyLevelAtTransportLayer);
        return this;
    }
}
