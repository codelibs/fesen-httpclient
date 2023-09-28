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
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.builder.SearchSourceBuilder;

public class HttpSearchAction extends HttpAction {

    protected final SearchAction action;

    public HttpSearchAction(final HttpClient client, final SearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        getCurlRequest(request).body(getQuerySource(request)).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                if (searchResponse.getHits() == null) {
                    listener.onFailure(toOpenSearchException(response, new OpenSearchException("hits is null.")));
                } else {
                    listener.onResponse(searchResponse);
                }
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected String getQuerySource(final SearchRequest request) {
        final SearchSourceBuilder source = request.source();
        if (source != null) {
            try {
                return XContentHelper.toXContent(source, XContentType.JSON, ToXContent.EMPTY_PARAMS, false).utf8ToString();
            } catch (final IOException e) {
                throw new OpenSearchException(e);
            }
        }
        return null;
    }

    protected CurlRequest getCurlRequest(final SearchRequest request) {
        // RestSearchAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_search", request.indices());
        curlRequest.param("typed_keys", "true");
        curlRequest.param("batched_reduce_size", Integer.toString(request.getBatchedReduceSize()));
        if (request.getPreFilterShardSize() != null) {
            curlRequest.param("pre_filter_shard_size", request.getPreFilterShardSize().toString());
        }
        if (request.getMaxConcurrentShardRequests() > 0) {
            curlRequest.param("max_concurrent_shard_requests", Integer.toString(request.getMaxConcurrentShardRequests()));
        }
        if (request.allowPartialSearchResults() != null) {
            curlRequest.param("allow_partial_search_results", request.allowPartialSearchResults().toString());
        }
        if (!SearchType.DEFAULT.equals(request.searchType())) {
            curlRequest.param("search_type", request.searchType().name().toLowerCase());
        }
        if (request.requestCache() != null) {
            curlRequest.param("request_cache", request.requestCache().toString());
        }
        if (request.scroll() != null) {
            curlRequest.param("scroll", request.scroll().keepAlive().toString());
        }
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        curlRequest.param("ccs_minimize_roundtrips", Boolean.toString(request.isCcsMinimizeRoundtrips()));
        return curlRequest;
    }
}
