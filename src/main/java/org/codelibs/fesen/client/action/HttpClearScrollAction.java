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
import org.codelibs.fesen.action.search.ClearScrollAction;
import org.codelibs.fesen.action.search.ClearScrollRequest;
import org.codelibs.fesen.action.search.ClearScrollResponse;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpClearScrollAction extends HttpAction {

    protected final ClearScrollAction action;

    public HttpClearScrollAction(final HttpClient client, final ClearScrollAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        String source = null;
        try (final XContentBuilder builder =
                XContentFactory.jsonBuilder().startObject().array("scroll_id", request.getScrollIds().toArray(new String[0])).endObject()) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a reqsuest.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClearScrollResponse clearScrollResponse = ClearScrollResponse.fromXContent(parser);
                listener.onResponse(clearScrollResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClearScrollRequest request) {
        // RestClearScrollAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_search/scroll");
        return curlRequest;
    }
}
