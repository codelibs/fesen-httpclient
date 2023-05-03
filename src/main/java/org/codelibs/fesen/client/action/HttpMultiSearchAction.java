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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.HttpClient.ContentType;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions.WildcardStates;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpMultiSearchAction extends HttpAction {

    protected final MultiSearchAction action;

    public HttpMultiSearchAction(final HttpClient client, final MultiSearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
        String source = null;
        try {
            source = new String(writeMultiLineFormat(request, XContentFactory.xContent(XContentType.JSON)));
        } catch (final Exception e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final MultiSearchResponse multiSearchResponse = MultiSearchResponse.fromXContext(parser);
                listener.onResponse(multiSearchResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    // MultiSearchRequest.writeMultiLineFormat(request, XContentFactory.xContent(XContentType.JSON))
    protected byte[] writeMultiLineFormat(final MultiSearchRequest multiSearchRequest, final XContent xContent) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (final SearchRequest request : multiSearchRequest.requests()) {
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                writeSearchRequestParams(request, xContentBuilder);
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.streamSeparator());
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                if (request.source() != null) {
                    request.source().toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                } else {
                    xContentBuilder.startObject();
                    xContentBuilder.endObject();
                }
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.streamSeparator());
        }
        return output.toByteArray();
    }

    //  MultiSearchRequest.writeSearchRequestParams(request, xContentBuilder)
    protected void writeSearchRequestParams(final SearchRequest request, final XContentBuilder xContentBuilder) throws IOException {
        final EngineType engineType = client.getEngineInfo().getType();
        if (engineType == EngineType.ELASTICSEARCH8 || engineType == EngineType.OPENSEARCH2) {
            xContentBuilder.startObject();
            if (request.indices() != null) {
                xContentBuilder.field("index", request.indices());
            }
            if (request.indicesOptions() != null && request.indicesOptions() != SearchRequest.DEFAULT_INDICES_OPTIONS) {
                WildcardStates.toXContent(request.indicesOptions().getExpandWildcards(), xContentBuilder);
                xContentBuilder.field("ignore_unavailable", request.indicesOptions().ignoreUnavailable());
                xContentBuilder.field("allow_no_indices", request.indicesOptions().allowNoIndices());
            }
            if (request.searchType() != null) {
                xContentBuilder.field("search_type", request.searchType().name().toLowerCase(Locale.ROOT));
            }
            xContentBuilder.field("ccs_minimize_roundtrips", request.isCcsMinimizeRoundtrips());
            if (request.requestCache() != null) {
                xContentBuilder.field("request_cache", request.requestCache());
            }
            if (request.preference() != null) {
                xContentBuilder.field("preference", request.preference());
            }
            if (request.routing() != null) {
                xContentBuilder.field("routing", request.routing());
            }
            if (request.allowPartialSearchResults() != null) {
                xContentBuilder.field("allow_partial_search_results", request.allowPartialSearchResults());
            }
            if (request.getCancelAfterTimeInterval() != null) {
                xContentBuilder.field("cancel_after_time_interval", request.getCancelAfterTimeInterval().getStringRep());
            }
            xContentBuilder.endObject();
        } else {
            MultiSearchRequest.writeSearchRequestParams(request, xContentBuilder);
        }
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
