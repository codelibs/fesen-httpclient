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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.liveness;

import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportChannel;
import org.codelibs.fesen.opensearch.transport.TransportRequestHandler;
import org.codelibs.fesen.opensearch.transport.TransportService;

/**
 * Transport action for OpenSearch Node Liveness
 *
 * @opensearch.internal
 */
public final class TransportLivenessAction implements TransportRequestHandler<LivenessRequest> {

    private final ClusterService clusterService;
    public static final String NAME = "cluster:monitor/nodes/liveness";

    @Inject
    public TransportLivenessAction(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME,
            false,
            false /*can not trip circuit breaker*/,
            LivenessRequest::new,
            this
        );
    }

    @Override
    public void messageReceived(LivenessRequest request, TransportChannel channel, Task task) throws Exception {
        channel.sendResponse(new LivenessResponse(clusterService.getClusterName(), clusterService.localNode()));
    }
}
