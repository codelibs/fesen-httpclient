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

import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.VersionType;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Handles the Get Document API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpGetAction extends HttpAction {

    /** The get action definition. */
    protected final GetAction action;

    /**
     * Creates a new HTTP get action.
     *
     * @param client the HTTP client used to send requests
     * @param action the get action definition
     */
    public HttpGetAction(final HttpClient client, final GetAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get request asynchronously.
     *
     * @param request the get request
     * @param listener the listener notified with the get response or a failure
     */
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

    /**
     * Builds the curl request for the get document API.
     *
     * @param request the get request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final GetRequest request) {
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
        final FetchSourceContext fetchSourceContext = request.fetchSourceContext();
        if (fetchSourceContext != null) {
            if (!fetchSourceContext.fetchSource()) {
                curlRequest.param("_source", "false");
            } else {
                if (fetchSourceContext.includes().length > 0) {
                    curlRequest.param("_source_includes", String.join(",", fetchSourceContext.includes()));
                }
                if (fetchSourceContext.excludes().length > 0) {
                    curlRequest.param("_source_excludes", String.join(",", fetchSourceContext.excludes()));
                }
            }
        }
        return curlRequest;
    }
}
