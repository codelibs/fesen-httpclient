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
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.shrink.ResizeResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the resize (shrink/split/clone) index API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpResizeAction extends HttpAction {

    /** The resize action. */
    protected final ResizeAction action;

    /**
     * Creates a new HTTP resize action.
     *
     * @param client the HTTP client
     * @param action the resize action
     */
    public HttpResizeAction(final HttpClient client, final ResizeAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the resize request asynchronously.
     *
     * @param request the resize request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ResizeRequest request, final ActionListener<ResizeResponse> listener) {
        final String source = toSource(request);
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ResizeResponse resizeResponse = ResizeResponse.fromXContent(parser);
                listener.onResponse(resizeResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Serializes the resize request body (settings, aliases, max_shard_size) to a JSON string.
     *
     * @param request the resize request
     * @return the JSON request body
     */
    protected String toSource(final ResizeRequest request) {
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
    }

    /**
     * Builds the curl request for the resize API.
     *
     * @param request the resize request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final ResizeRequest request) {
        // RestResizeHandler
        final String path = "/_" + request.getResizeType().name().toLowerCase(Locale.ROOT) + "/"
                + UrlUtils.encode(request.getTargetIndexRequest().index());
        final CurlRequest curlRequest = client.getCurlRequest(POST, path, request.getSourceIndex());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        final ActiveShardCount waitForActiveShards = request.getTargetIndexRequest().waitForActiveShards();
        if (!ActiveShardCount.DEFAULT.equals(waitForActiveShards)) {
            curlRequest.param("wait_for_active_shards", getActiveShardsCountString(waitForActiveShards));
        }
        return curlRequest;
    }
}
