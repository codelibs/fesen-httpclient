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

package org.codelibs.fesen.opensearch.action.support.tasks;

import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * A base class for task requests
 *
 * @opensearch.internal
 */
public class BaseTasksRequest<Request extends BaseTasksRequest<Request>> extends ActionRequest {

    public static final String[] ALL_ACTIONS = Strings.EMPTY_ARRAY;

    public static final String[] ALL_NODES = Strings.EMPTY_ARRAY;

    private String[] nodes = ALL_NODES;

    private TimeValue timeout;

    private String[] actions = ALL_ACTIONS;

    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    // NOTE: This constructor is only needed, because the setters in this class,
    // otherwise it can be removed and above fields can be made final.
    public BaseTasksRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        parentTaskId.writeTo(out);
        out.writeStringArrayNullable(nodes);
        out.writeStringArrayNullable(actions);
        out.writeOptionalTimeValue(timeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() && nodes.length > 0) {
            validationException = addValidationError("task id cannot be used together with node ids", validationException);
        }
        return validationException;
    }

    /**
     * Return the list of action masks for the actions that should be returned
     */
    public String[] getActions() {
        return actions;
    }

    public final String[] getNodes() {
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public final Request setNodes(String... nodes) {
        this.nodes = nodes;
        return (Request) this;
    }

    /**
     * Returns the id of the task that should be processed.
     * <p>
     * By default tasks with any ids are returned.
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    public boolean match(Task task) {
        if (CollectionUtils.isEmpty(getActions()) == false && Regex.simpleMatch(getActions(), task.getAction()) == false) {
            return false;
        }
        if (getTaskId().isSet()) {
            if (getTaskId().getId() != task.getId()) {
                return false;
            }
        }
        if (parentTaskId.isSet()) {
            if (parentTaskId.equals(task.getParentTaskId()) == false) {
                return false;
            }
        }
        return true;
    }
}
