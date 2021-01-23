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
import org.codelibs.fesen.client.HttpClient.ContentType;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.search.MultiSearchAction;
import org.codelibs.fesen.action.search.MultiSearchRequest;
import org.codelibs.fesen.action.search.MultiSearchResponse;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.XContentType;

public class HttpMultiSearchAction extends HttpAction {

    protected final MultiSearchAction action;

    public HttpMultiSearchAction(final HttpClient client, final MultiSearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
        String source = null;
        try {
            source = new String(MultiSearchRequest.writeMultiLineFormat(request, XContentFactory.xContent(XContentType.JSON)));
        } catch (final Exception e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MultiSearchResponse multiSearchResponse = MultiSearchResponse.fromXContext(parser);
                listener.onResponse(multiSearchResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final MultiSearchRequest request) {
        // RestMultiSearchAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, ContentType.X_NDJSON, "/_msearch");
        if (request.maxConcurrentSearchRequests() > 0) {
            curlRequest.param("max_concurrent_searches", Integer.toString(request.maxConcurrentSearchRequests()));
        }
        return curlRequest;
    }
}
