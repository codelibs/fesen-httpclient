/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.task.commons.worker;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.task.commons.task.Task;

/**
 * Task Worker that executes the Task
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskWorker {

    /**
     * Execute the Task
     *
     * @param task Task to be executed
     */
    void executeTask(Task task);

}
