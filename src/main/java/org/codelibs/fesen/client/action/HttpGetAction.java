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

import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.VersionType;

public class HttpGetAction extends HttpAction {

    protected final GetAction action;

    public HttpGetAction(final HttpClient client, final GetAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetRequest request, final ActionListener<GetResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetResponse getResponse = GetResponse.fromXContent(parser);
                listener.onResponse(getResponse);
            } catch (final Exception e) {
                if (response.getHttpStatusCode() == 404) {
                    throw new IndexNotFoundException(request.index(), e);
                }
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    private CurlRequest getCurlRequest(final GetRequest request) {
        // RestGetAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_doc/" + UrlUtils.encode(request.id()), request.index());
        if (request.refresh()) {
            curlRequest.param("refresh", "true");
        }
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        if (request.realtime()) {
            curlRequest.param("realtime", "true");
        }
        if (request.storedFields() != null) {
            curlRequest.param("stored_fields", String.join(",", request.storedFields()));
        }
        if (request.version() >= 0) {
            curlRequest.param("version", Long.toString(request.version()));
        }
        if (!VersionType.INTERNAL.equals(request.versionType())) {
            curlRequest.param("version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        return curlRequest;
    }
}
