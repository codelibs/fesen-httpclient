/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.rest.action.admin.cluster;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.tasks.CancellableTask;

import java.util.Map;

import static org.codelibs.fesen.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Task storing information about a currently running ClusterRequest.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.17.0")
public class ClusterAdminTask extends CancellableTask {

    public ClusterAdminTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, parentTaskId, headers, NO_TIMEOUT);
    }

    public ClusterAdminTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        super(id, type, action, null, parentTaskId, headers, cancelAfterTimeInterval);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }
}
