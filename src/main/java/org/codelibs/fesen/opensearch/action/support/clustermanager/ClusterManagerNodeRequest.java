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

import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A based request for cluster-manager based operation.
 *
 * @param <Request> the request type
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ClusterManagerNodeRequest<Request extends ClusterManagerNodeRequest<Request>> extends ActionRequest {

    /**
     * The DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT constant.
     */
    public static final TimeValue DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT} */
    @Deprecated
    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    /**
     * The cluster manager node timeout.
     */
    protected TimeValue clusterManagerNodeTimeout = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout} */
    @Deprecated
    protected TimeValue masterNodeTimeout = clusterManagerNodeTimeout;

    /**
     * Creates a new ClusterManagerNodeRequest.
     */
    protected ClusterManagerNodeRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(clusterManagerNodeTimeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     *
     * @param timeout the timeout
     * @return the cluster manager node timeout
     */
    @SuppressWarnings("unchecked")
    public final Request clusterManagerNodeTimeout(TimeValue timeout) {
        this.clusterManagerNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     *
     * @param timeout the timeout
     * @return the master node timeout
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout(TimeValue)}
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public final Request masterNodeTimeout(TimeValue timeout) {
        return clusterManagerNodeTimeout(timeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     *
     * @param timeout the timeout
     * @return the cluster manager node timeout
     */
    public final Request clusterManagerNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(
            TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".clusterManagerNodeTimeout")
        );
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     *
     * @param timeout the timeout
     * @return the master node timeout
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout(String)}
     */
    @Deprecated
    public final Request masterNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(timeout);
    }

    /**
     * Returns the cluster manager node timeout.
     *
     * @return the cluster manager node timeout
     */
    public final TimeValue clusterManagerNodeTimeout() {
        return this.clusterManagerNodeTimeout;
    }

    /**
     * @return the master node timeout
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout()}
     */
    @Deprecated
    public final TimeValue masterNodeTimeout() {
        return clusterManagerNodeTimeout();
    }
}
