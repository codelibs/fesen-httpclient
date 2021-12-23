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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.get.GetIndexAction;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.common.xcontent.XContentParser;

public class HttpGetIndexAction extends HttpAction {

    protected final GetIndexAction action;

    public HttpGetIndexAction(final HttpClient client, final GetIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexRequest request, final ActionListener<GetIndexResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIndexResponse getIndexResponse = GetIndexResponse.fromXContent(parser);
                listener.onResponse(getIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetIndexRequest request) {
        // RestGetIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/", request.indices());
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        return curlRequest;
    }
}
