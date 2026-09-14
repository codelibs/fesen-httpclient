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

import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Transport request for OpenSearch Hot Threads
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesHotThreadsRequest extends BaseNodesRequest<NodesHotThreadsRequest> {

    int threads = 3;
    String type = "cpu";
    TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    int snapshots = 10;
    boolean ignoreIdleThreads = true;

    /**
     * Get hot threads from nodes based on the nodes ids specified. If none are passed, hot
     * threads for all nodes is used.
     *
     * @param nodesIds the nodes identifiers
     */
    public NodesHotThreadsRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Returns the threads.
     *
     * @return the threads
     */
    public int threads() {
        return this.threads;
    }

    /**
     * Returns the threads.
     *
     * @param threads the threads
     * @return the threads
     */
    public NodesHotThreadsRequest threads(int threads) {
        this.threads = threads;
        return this;
    }

    /**
     * Returns the ignore idle threads.
     *
     * @return the ignore idle threads
     */
    public boolean ignoreIdleThreads() {
        return this.ignoreIdleThreads;
    }

    /**
     * Returns the ignore idle threads.
     *
     * @param ignoreIdleThreads the ignore idle threads
     * @return the ignore idle threads
     */
    public NodesHotThreadsRequest ignoreIdleThreads(boolean ignoreIdleThreads) {
        this.ignoreIdleThreads = ignoreIdleThreads;
        return this;
    }

    /**
     * Returns the type.
     *
     * @param type the type
     * @return the type
     */
    public NodesHotThreadsRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns the type.
     *
     * @return the type
     */
    public String type() {
        return this.type;
    }

    /**
     * Returns the interval.
     *
     * @param interval the interval
     * @return the interval
     */
    public NodesHotThreadsRequest interval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Returns the interval.
     *
     * @return the interval
     */
    public TimeValue interval() {
        return this.interval;
    }

    /**
     * Returns the snapshots.
     *
     * @return the snapshots
     */
    public int snapshots() {
        return this.snapshots;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(threads);
        out.writeBoolean(ignoreIdleThreads);
        out.writeString(type);
        out.writeTimeValue(interval);
        out.writeInt(snapshots);
    }
}
