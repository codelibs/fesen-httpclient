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
import org.codelibs.fesen.action.get.MultiGetAction;
import org.codelibs.fesen.action.get.MultiGetRequest;
import org.codelibs.fesen.action.get.MultiGetResponse;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.json.JsonXContent;

public class HttpMultiGetAction extends HttpAction {

    protected final MultiGetAction action;

    public HttpMultiGetAction(final HttpClient client, final MultiGetAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MultiGetResponse multiGetResponse = MultiGetResponse.fromXContent(parser);
                listener.onResponse(multiGetResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final MultiGetRequest request) {
        // RestMultiGetAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mget");
        curlRequest.param("refresh", Boolean.toString(request.refresh()));
        curlRequest.param("realtime", Boolean.toString(request.realtime()));
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        return curlRequest;
    }
}
