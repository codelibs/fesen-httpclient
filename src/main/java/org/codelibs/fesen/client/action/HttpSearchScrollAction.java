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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.action.search.SearchScrollAction;
import org.codelibs.fesen.action.search.SearchScrollRequest;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.json.JsonXContent;

public class HttpSearchScrollAction extends HttpAction {

    protected final SearchScrollAction action;

    public HttpSearchScrollAction(final HttpClient client, final SearchScrollAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SearchResponse scrollResponse = SearchResponse.fromXContent(parser);
                listener.onResponse(scrollResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SearchScrollRequest request) {
        // RestSearchScrollAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_search/scroll");
        if (request.scrollId() != null) {
            curlRequest.param("scroll_id", request.scrollId());
        }
        if (request.scroll() != null) {
            curlRequest.param("scroll", request.scroll().keepAlive().toString());
        }
        return curlRequest;
    }
}
