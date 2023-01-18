/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.open.OpenIndexAction;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.xcontent.XContentParser;

public class HttpOpenIndexAction extends HttpAction {

    protected final OpenIndexAction action;

    public HttpOpenIndexAction(final HttpClient client, final OpenIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final OpenIndexResponse openIndexResponse = OpenIndexResponse.fromXContent(parser);
                listener.onResponse(openIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final OpenIndexRequest request) {
        // RestOpenIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_open", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", request.waitForActiveShards().toString());
        }
        return curlRequest;
    }
}
