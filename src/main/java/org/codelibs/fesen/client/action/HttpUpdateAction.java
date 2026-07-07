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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.action.update.UpdateResponse.Builder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the document update API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpUpdateAction extends HttpAction {

    /** The update action definition. */
    protected final UpdateAction action;

    /**
     * Creates a new HTTP update action.
     *
     * @param client the HTTP client used to send requests
     * @param action the update action definition
     */
    public HttpUpdateAction(final HttpClient client, final UpdateAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the update request and notifies the listener with the response.
     *
     * @param request the update request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final UpdateResponse updateResponse = fromXContent(parser);
                listener.onResponse(updateResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses an update response from the given parser.
     *
     * @param parser the parser positioned at the response content
     * @return the parsed update response
     * @throws IOException if parsing fails
     */
    // UpdateResponse.fromXContent(parser)
    protected UpdateResponse fromXContent(final XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        final Builder context = new Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            UpdateResponse.parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Builds the curl request for the update API.
     *
     * @param request the update request
     * @return the curl request for the update endpoint
     */
    protected CurlRequest getCurlRequest(final UpdateRequest request) {
        // RestUpdateAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_update/" + UrlUtils.encode(request.id()), request.index());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", getActiveShardsCountString(request.waitForActiveShards()));
        }
        curlRequest.param("doc_as_upsert", Boolean.toString(request.docAsUpsert()));
        curlRequest.param("retry_on_conflict", String.valueOf(request.retryOnConflict()));
        curlRequest.param("if_seq_no", Long.toString(request.ifSeqNo()));
        curlRequest.param("if_primary_term", Long.toString(request.ifPrimaryTerm()));
        return curlRequest;
    }
}
