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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete.DeleteTaskAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete.DeleteTaskRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

/**
 * Handles the delete task API over HTTP for OpenSearch,
 * removing the stored result of a completed task.
 */
public class HttpDeleteTaskAction extends HttpAction {

    /** The delete task action definition. */
    protected final DeleteTaskAction action;

    /**
     * Creates a new HttpDeleteTaskAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the delete task action definition
     */
    public HttpDeleteTaskAction(final HttpClient client, final DeleteTaskAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the delete task request asynchronously and notifies the listener with the result.
     *
     * @param request the delete task request
     * @param listener the listener to notify with the acknowledged response or a failure
     */
    public void execute(final DeleteTaskRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse deleteTaskResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(deleteTaskResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the HTTP request for the delete task request.
     *
     * @param request the delete task request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeleteTaskRequest request) {
        // RestDeleteTaskAction
        final String taskId = request.getTaskId().getNodeId() + ":" + request.getTaskId().getId();
        return client.getCurlRequest(DELETE, "/_tasks/" + taskId);
    }
}
