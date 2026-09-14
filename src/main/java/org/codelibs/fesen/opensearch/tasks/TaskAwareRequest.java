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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.tasks;

import org.codelibs.fesen.opensearch.core.tasks.TaskId;

import java.util.Map;

/**
 * An interface for a request that can be used to register a task manager task
 *
 * @opensearch.internal
 */
public interface TaskAwareRequest {
    /**
     * Set a reference to task that caused this task to be run.
     *
     * @param parentTaskNode the parent task node
     * @param parentTaskId the parent task identifier
     */
    default void setParentTask(String parentTaskNode, long parentTaskId) {
        setParentTask(new TaskId(parentTaskNode, parentTaskId));
    }

    /**
     * Set a reference to task that created this request.
     *
     * @param taskId the task identifier
     */
    void setParentTask(TaskId taskId);

    /**
     * Get a reference to the task that created this request. Implementers should default to
     * {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     *
     * @return the parent task
     */
    TaskId getParentTask();

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param parentTaskId the parent task identifier
     * @param headers the headers
     * @return the new task
     */
    default Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new Task(id, type, action, getDescription(), parentTaskId, headers);
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
     *
     * @return the description
     */
    default String getDescription() {
        return "";
    }
}
