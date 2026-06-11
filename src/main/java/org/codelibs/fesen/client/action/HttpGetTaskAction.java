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

/**
 * Handles the get task API over HTTP for OpenSearch/Elasticsearch,
 * retrieving information about a task currently running in the cluster.
 */
public class HttpGetTaskAction extends HttpAction {

    /** The get task action. */
    protected final GetTaskAction action;

    /**
     * Creates a new HttpGetTaskAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the get task action
     */
    public HttpGetTaskAction(final HttpClient client, final GetTaskAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get task request and notifies the listener with the response.
     *
     * @param request the get task request
     * @param listener the listener to notify with the response or a failure
     */
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

    /**
     * Parses a get task response from the given parser.
     *
     * @param parser the parser for the response body
     * @return the parsed get task response
     * @throws IOException if the response cannot be parsed
     */
    protected GetTaskResponse fromXContent(final XContentParser parser) throws IOException {
        parser.nextToken();
        final TaskResult taskResult = TaskResult.PARSER.apply(parser, null);
        return new GetTaskResponse(taskResult);
    }

    /**
     * Builds the curl request for the get task API.
     *
     * @param request the get task request
     * @return the curl request
     */
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
