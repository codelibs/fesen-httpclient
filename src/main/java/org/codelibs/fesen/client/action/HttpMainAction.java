/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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
import org.opensearch.action.main.MainAction;
import org.opensearch.action.main.MainRequest;
import org.opensearch.action.main.MainResponse;
import org.opensearch.common.xcontent.XContentParser;

public class HttpMainAction extends HttpAction {

    protected final MainAction action;

    public HttpMainAction(final HttpClient client, final MainAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MainRequest request, final ActionListener<MainResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MainResponse mainResponse = MainResponse.fromXContent(parser);
                listener.onResponse(mainResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final MainRequest request) {
        // RestMainAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_xpack");
        return curlRequest;
    }
}
