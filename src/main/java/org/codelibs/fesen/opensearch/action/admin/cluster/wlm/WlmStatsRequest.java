/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.wlm;

import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A request to get Workload Management Stats
 */
@ExperimentalApi
public class WlmStatsRequest extends BaseNodesRequest<WlmStatsRequest> {

    private final Set<String> workloadGroupIds;
    private final Boolean breach;

    /**
     * Get WorkloadGroup stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     *
     * @param nodesIds the nodes identifiers
     * @param workloadGroupIds the workload group identifiers
     * @param breach the breach
     */
    public WlmStatsRequest(String[] nodesIds, Set<String> workloadGroupIds, Boolean breach) {
        super(nodesIds);
        this.workloadGroupIds = workloadGroupIds;
        this.breach = breach;
    }

    /**
     * Creates a new WlmStatsRequest.
     */
    public WlmStatsRequest() {
        super((String[]) null);
        workloadGroupIds = new HashSet<>();
        this.breach = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(workloadGroupIds.toArray(new String[0]));
        out.writeOptionalBoolean(breach);
    }

    /**
     * Returns the workload group identifiers.
     *
     * @return the workload group identifiers
     */
    public Set<String> getWorkloadGroupIds() {
        return workloadGroupIds;
    }

    /**
     * Returns the breach flag.
     *
     * @return the breach flag
     */
    public Boolean isBreach() {
        return breach;
    }
}
