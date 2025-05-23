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
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpListTasksAction extends HttpAction {

    protected final ListTasksAction action;

    public HttpListTasksAction(final HttpClient client, final ListTasksAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ListTasksRequest request, final ActionListener<ListTasksResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ListTasksResponse listTasksResponse = ListTasksResponse.fromXContent(parser);
                listener.onResponse(listTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ListTasksRequest request) {
        // RestListTasksAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_tasks");
        curlRequest.param("detailed", String.valueOf(request.getDetailed()));
        curlRequest.param("parent_task_id", String.valueOf(request.getParentTaskId()));
        curlRequest.param("wait_for_completion", String.valueOf(request.getWaitForCompletion()));
        if (request.getNodes() != null) {
            curlRequest.param("nodes", String.join(",", request.getNodes()));
        }
        if (request.getActions() != null) {
            curlRequest.param("actions", String.join(",", request.getActions()));
        }
        return curlRequest;
    }
}
