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

package org.codelibs.fesen.opensearch.action.admin.cluster.reroute;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to submit cluster reroute allocation commands
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterRerouteRequest extends AcknowledgedRequest<ClusterRerouteRequest> {
    private AllocationCommands commands = new AllocationCommands();
    private boolean dryRun;
    private boolean explain;
    private boolean retryFailed;

    public ClusterRerouteRequest() {}

    /**
     * Returns the current dry run flag which allows to run the commands without actually applying them,
     * just to get back the resulting cluster state back.
     */
    public boolean dryRun() {
        return this.dryRun;
    }

    /**
     * Returns the current explain flag
     */
    public boolean explain() {
        return this.explain;
    }

    /**
     * Returns the current retry failed flag
     */
    public boolean isRetryFailed() {
        return this.retryFailed;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        AllocationCommands.writeTo(commands, out);
        out.writeBoolean(dryRun);
        out.writeBoolean(explain);
        out.writeBoolean(retryFailed);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClusterRerouteRequest other = (ClusterRerouteRequest) obj;
        // Override equals and hashCode for testing
        return Objects.equals(commands, other.commands)
            && Objects.equals(dryRun, other.dryRun)
            && Objects.equals(explain, other.explain)
            && Objects.equals(timeout, other.timeout)
            && Objects.equals(retryFailed, other.retryFailed)
            && Objects.equals(clusterManagerNodeTimeout, other.clusterManagerNodeTimeout);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(commands, dryRun, explain, timeout, retryFailed, clusterManagerNodeTimeout);
    }
}
