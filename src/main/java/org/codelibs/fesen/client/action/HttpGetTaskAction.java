/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action;

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.TaskResult;

public class HttpGetTaskAction extends HttpAction {

    protected final GetTaskAction action;

    public HttpGetTaskAction(final HttpClient client, final GetTaskAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetTaskRequest request, final ActionListener<GetTaskResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetTaskResponse getTaskResponse = fromXContent(parser);
                listener.onResponse(getTaskResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected GetTaskResponse fromXContent(final XContentParser parser) throws IOException {
        parser.nextToken();
        final TaskResult taskResult = TaskResult.PARSER.apply(parser, null);
        return new GetTaskResponse(taskResult);
    }

    protected CurlRequest getCurlRequest(final GetTaskRequest request) {
        final String taskId = request.getTaskId().getNodeId() + ":" + request.getTaskId().getId();
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_tasks/" + taskId);
        if (request.getWaitForCompletion()) {
            curlRequest.param("wait_for_completion", "true");
        }
        if (request.getTimeout() != null) {
            curlRequest.param("timeout", request.getTimeout().toString());
        }
        return curlRequest;
    }
}
