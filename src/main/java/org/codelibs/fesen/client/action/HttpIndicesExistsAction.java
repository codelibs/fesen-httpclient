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
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;

public class HttpIndicesExistsAction extends HttpAction {

    protected final IndicesExistsAction action;

    public HttpIndicesExistsAction(final HttpClient client, final IndicesExistsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            final boolean exists = switch (response.getHttpStatusCode()) {
            case 200 -> true;
            case 404 -> false;
            default -> throw new OpenSearchException("Unexpected status: " + response.getHttpStatusCode());
            };
            try {
                final IndicesExistsResponse indicesExistsResponse = new IndicesExistsResponse(exists);
                listener.onResponse(indicesExistsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final IndicesExistsRequest request) {
        return client.getCurlRequest(HEAD, null, request.indices());
    }
}
