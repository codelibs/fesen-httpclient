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
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the validate query API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpValidateQueryAction extends HttpAction {

    /** The validate query action definition. */
    protected final ValidateQueryAction action;

    /**
     * Creates a new HTTP validate query action.
     *
     * @param client the HTTP client used to send requests
     * @param action the validate query action definition
     */
    public HttpValidateQueryAction(final HttpClient client, final ValidateQueryAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the validate query request and notifies the listener with the response.
     *
     * @param request the validate query request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        String source = null;
        try (final XContentBuilder builder =
                XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject()) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ValidateQueryResponse validateQueryResponse = ValidateQueryResponse.fromXContent(parser);
                listener.onResponse(validateQueryResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the validate query API.
     *
     * @param request the validate query request
     * @return the curl request for the validate query endpoint
     */
    protected CurlRequest getCurlRequest(final ValidateQueryRequest request) {
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_validate/query", request.indices());
        curlRequest.param("explain", Boolean.toString(request.explain()));
        curlRequest.param("rewrite", Boolean.toString(request.rewrite()));
        curlRequest.param("all_shards", Boolean.toString(request.allShards()));
        return curlRequest;
    }
}
