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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

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
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MultiGetResponse multiGetResponse = MultiGetResponse.fromXContent(parser);
                listener.onResponse(multiGetResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
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
