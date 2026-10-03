/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for the request to delete a stored completed task result.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DeleteTaskRequestBuilder extends ActionRequestBuilder<DeleteTaskRequest, AcknowledgedResponse> {
    /**
     * Creates a new DeleteTaskRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public DeleteTaskRequestBuilder(OpenSearchClient client, DeleteTaskAction action) {
        super(client, action, new DeleteTaskRequest());
    }

    /**
     * Set the TaskId to delete. Required.
     *
     * @param taskId the task identifier
     * @return this instance
     */
    public final DeleteTaskRequestBuilder setTaskId(TaskId taskId) {
        request.setTaskId(taskId);
        return this;
    }
}
