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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the search scroll API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpSearchScrollAction extends HttpAction {

    /** The search scroll action. */
    protected final SearchScrollAction action;

    /**
     * Creates a new instance.
     *
     * @param client the HTTP client
     * @param action the search scroll action
     */
    public HttpSearchScrollAction(final HttpClient client, final SearchScrollAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the search scroll request and notifies the listener with the response.
     *
     * @param request the search scroll request
     * @param listener the listener to be notified with the search response or a failure
     */
    public void execute(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SearchResponse scrollResponse = SearchResponse.fromXContent(parser);
                listener.onResponse(scrollResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds a curl request for the search scroll request.
     *
     * @param request the search scroll request
     * @return the curl request
     */
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
