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
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the indices aliases API over HTTP for OpenSearch/Elasticsearch,
 * adding or removing index aliases.
 */
public class HttpIndicesAliasesAction extends HttpAction {

    /** The indices aliases action. */
    protected final IndicesAliasesAction action;

    /**
     * Creates a new HttpIndicesAliasesAction.
     *
     * @param client the HTTP client to send requests with
     * @param action the indices aliases action
     */
    public HttpIndicesAliasesAction(final HttpClient client, final IndicesAliasesAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the indices aliases request and notifies the listener with the response.
     *
     * @param request the indices aliases request
     * @param listener the listener to notify with the response or a failure
     */
    public void execute(final IndicesAliasesRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse indicesAliasesResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(indicesAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the indices aliases API.
     *
     * @param request the indices aliases request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final IndicesAliasesRequest request) {
        // RestIndicesAliasesAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_aliases");
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        String source = null;
        try (final XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        return curlRequest.body(source);
    }
}
