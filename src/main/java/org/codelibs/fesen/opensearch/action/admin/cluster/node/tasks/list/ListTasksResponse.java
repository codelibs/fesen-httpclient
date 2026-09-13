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

package org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.TaskOperationFailure;
import org.codelibs.fesen.opensearch.action.support.tasks.BaseTasksResponse;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodeRole;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.opensearch.common.TriFunction;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Returns the list of tasks currently running on the nodes
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ListTasksResponse extends BaseTasksResponse implements ToXContentObject {
    private static final String TASKS = "tasks";

    private final List<TaskInfo> tasks;

    public ListTasksResponse(
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskFailures,
        List<? extends OpenSearchException> nodeFailures
    ) {
        super(taskFailures, nodeFailures);
        this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
    }

    public ListTasksResponse(StreamInput in) throws IOException {
        super(in);
        tasks = Collections.unmodifiableList(in.readList(TaskInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(tasks);
    }

    protected static <T> ConstructingObjectParser<T, Void> setupParser(
        String name,
        TriFunction<List<TaskInfo>, List<TaskOperationFailure>, List<OpenSearchException>, T> ctor
    ) {
        ConstructingObjectParser<T, Void> parser = new ConstructingObjectParser<>(name, true, constructingObjects -> {
            int i = 0;
            @SuppressWarnings("unchecked")
            List<TaskInfo> tasks = (List<TaskInfo>) constructingObjects[i++];
            @SuppressWarnings("unchecked")
            List<TaskOperationFailure> tasksFailures = (List<TaskOperationFailure>) constructingObjects[i++];
            @SuppressWarnings("unchecked")
            List<OpenSearchException> nodeFailures = (List<OpenSearchException>) constructingObjects[i];
            return ctor.apply(tasks, tasksFailures, nodeFailures);
        });
        parser.declareObjectArray(optionalConstructorArg(), TaskInfo.PARSER, new ParseField(TASKS));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p), new ParseField(TASK_FAILURES));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> OpenSearchException.fromXContent(p), new ParseField(NODE_FAILURES));
        return parser;
    }

    private static final ConstructingObjectParser<ListTasksResponse, Void> PARSER = setupParser(
        "list_tasks_response",
        ListTasksResponse::new
    );

    /**
     * Get the tasks found by this request.
     */
    public List<TaskInfo> getTasks() {
        return tasks;
    }

    /**
     * Presents a flat list of tasks
     */
    public XContentBuilder toXContentGroupedByNone(XContentBuilder builder, Params params) throws IOException {
        toXContentCommon(builder, params);
        builder.startArray(TASKS);
        for (TaskInfo taskInfo : getTasks()) {
            builder.startObject();
            taskInfo.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentGroupedByNone(builder, params);
        builder.endObject();
        return builder;
    }

    public static ListTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
