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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the delete snapshot API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpDeleteSnapshotAction extends HttpAction {

    /** The delete snapshot action definition. */
    protected final DeleteSnapshotAction action;

    /**
     * Creates a new HttpDeleteSnapshotAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the delete snapshot action definition
     */
    public HttpDeleteSnapshotAction(final HttpClient client, final DeleteSnapshotAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the delete snapshot request asynchronously and notifies the listener with the result.
     *
     * @param request the delete snapshot request
     * @param listener the listener to notify with the acknowledged response or a failure
     */
    public void execute(final DeleteSnapshotRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse cancelTasksResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the HTTP request for the delete snapshot request.
     *
     * @param request the delete snapshot request
     * @return the configured curl request
     */
    protected CurlRequest getCurlRequest(final DeleteSnapshotRequest request) {
        // RestDeleteSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        }
        if (request.snapshots() != null && request.snapshots().length > 0) {
            pathBuf.append('/').append(UrlUtils.encode(Arrays.stream(request.snapshots()).collect(Collectors.joining(","))));
        }
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
