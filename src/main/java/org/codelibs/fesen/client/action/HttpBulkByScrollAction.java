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
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.AbstractBulkByScrollRequest;
import org.opensearch.index.reindex.BulkByScrollResponse;

/**
 * Base class for HTTP actions that return a {@link BulkByScrollResponse}, namely
 * the reindex, update-by-query, and delete-by-query APIs. Provides shared response
 * parsing, request-body serialization, and handling of the query parameters that are
 * common to all bulk-by-scroll requests.
 */
public abstract class HttpBulkByScrollAction extends HttpAction {

    /**
     * Creates a new bulk-by-scroll HTTP action.
     *
     * @param client the HTTP client used to send requests
     */
    protected HttpBulkByScrollAction(final HttpClient client) {
        super(client);
    }

    /**
     * Parses a {@link BulkByScrollResponse} from the given XContent parser.
     *
     * @param parser the parser positioned at the response body
     * @return the parsed bulk-by-scroll response
     */
    protected BulkByScrollResponse fromXContent(final XContentParser parser) {
        // BulkByScrollResponse.fromXContent(parser)
        return BulkByScrollResponse.fromXContent(parser);
    }

    /**
     * Serializes a request that implements {@link ToXContentObject} into a JSON string
     * to be used as the HTTP request body.
     *
     * @param request the request to serialize
     * @return the JSON representation of the request
     */
    protected String toSource(final ToXContentObject request) {
        try {
            return XContentHelper.toXContent(request, XContentType.JSON, ToXContent.EMPTY_PARAMS, false).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to build a request.", e);
        }
    }

    /**
     * Adds the query parameters shared by all bulk-by-scroll requests. The response is
     * always requested synchronously by forcing {@code wait_for_completion=true} so that
     * the response body can be parsed as a {@link BulkByScrollResponse}.
     *
     * @param curlRequest the curl request to add parameters to
     * @param request the bulk-by-scroll request providing the parameter values
     */
    protected void setCommonParams(final CurlRequest curlRequest, final AbstractBulkByScrollRequest<?> request) {
        // AbstractBaseReindexRestHandler#setCommonOptions
        curlRequest.param("wait_for_completion", "true");
        if (request.isRefresh()) {
            curlRequest.param("refresh", "true");
        }
        if (request.getTimeout() != null) {
            curlRequest.param("timeout", request.getTimeout().toString());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.getWaitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", getActiveShardsCountString(request.getWaitForActiveShards()));
        }
        final float requestsPerSecond = request.getRequestsPerSecond();
        if (!Float.isInfinite(requestsPerSecond)) {
            curlRequest.param("requests_per_second", Float.toString(requestsPerSecond));
        }
        final int slices = request.getSlices();
        if (slices != 1) {
            curlRequest.param("slices", slices == AbstractBulkByScrollRequest.AUTO_SLICES ? AbstractBulkByScrollRequest.AUTO_SLICES_VALUE
                    : Integer.toString(slices));
        }
    }
}
