/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete;

import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to delete a stored completed task result.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DeleteTaskRequest extends ActionRequest {
    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    /**
     * Creates a new DeleteTaskRequest.
     */
    public DeleteTaskRequest() {}

    /**
     * Creates a new DeleteTaskRequest by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public DeleteTaskRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
    }

    /**
     * Returns the TaskId to delete.
     *
     * @return the task identifier
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Set the TaskId to delete. Required.
     *
     * @param taskId the task identifier
     * @return this instance
     */
    public DeleteTaskRequest setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (false == getTaskId().isSet()) {
            validationException = addValidationError("task id is required", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
    }
}
