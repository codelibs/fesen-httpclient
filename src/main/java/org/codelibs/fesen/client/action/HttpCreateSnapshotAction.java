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
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpCreateSnapshotAction extends HttpAction {

    protected final CreateSnapshotAction action;

    public HttpCreateSnapshotAction(final HttpClient client, final CreateSnapshotAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CreateSnapshotRequest request, final ActionListener<CreateSnapshotResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreateSnapshotResponse cancelTasksResponse = CreateSnapshotResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final CreateSnapshotRequest request) {
        // RestCreateSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        }
        if (request.snapshot() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.snapshot()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(POST, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        curlRequest.param("wait_for_completion", String.valueOf(request.waitForCompletion()));
        return curlRequest;
    }
}
