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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel;

import org.codelibs.fesen.opensearch.action.support.tasks.BaseTasksRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.tasks.CancellableTask;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to cancel tasks
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CancelTasksRequest extends BaseTasksRequest<CancelTasksRequest> {

    public static final String DEFAULT_REASON = "by user request";
    public static final boolean DEFAULT_WAIT_FOR_COMPLETION = false;

    private String reason = DEFAULT_REASON;
    private boolean waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION;

    public CancelTasksRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public boolean match(Task task) {
        return super.match(task) && task instanceof CancellableTask;
    }

    /**
     * The reason for canceling the task.
     */
    public String getReason() {
        return reason;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public String getDescription() {
        return "reason["
            + reason
            + "], waitForCompletion["
            + waitForCompletion
            + "], taskId["
            + getTaskId()
            + "], parentTaskId["
            + getParentTaskId()
            + "], nodes"
            + Arrays.toString(getNodes())
            + ", actions"
            + Arrays.toString(getActions());
    }
}
