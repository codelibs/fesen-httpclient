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
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.view.GetViewAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpGetViewAction extends HttpAction {

    protected final GetViewAction action;

    public HttpGetViewAction(final HttpClient client, final GetViewAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetViewAction.Response getViewResponse = GetViewAction.Response.fromXContent(parser);
                listener.onResponse(getViewResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetViewAction.Request request) {
        // RestViewAction
        return client.getCurlRequest(GET, "/views/" + UrlUtils.encode(request.getName()));
    }
}
