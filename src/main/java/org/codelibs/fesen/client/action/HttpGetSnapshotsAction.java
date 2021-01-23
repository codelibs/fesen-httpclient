/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpGetSnapshotsAction extends HttpAction {

    protected final GetSnapshotsAction action;

    public HttpGetSnapshotsAction(final HttpClient client, final GetSnapshotsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetSnapshotsRequest request, final ActionListener<GetSnapshotsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetSnapshotsResponse cancelTasksResponse = GetSnapshotsResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetSnapshotsRequest request) {
        // RestGetSnapshotsAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        } else {
            pathBuf.append("/_all");
        }
        if (request.snapshots() != null && request.snapshots().length > 0) {
            pathBuf.append('/').append(UrlUtils.joinAndEncode(",", request.snapshots()));
        } else {
            pathBuf.append("/_all");
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathBuf.toString());
        curlRequest.param("ignore_unavailable", String.valueOf(request.ignoreUnavailable()));
        curlRequest.param("verbose", String.valueOf(request.verbose()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
