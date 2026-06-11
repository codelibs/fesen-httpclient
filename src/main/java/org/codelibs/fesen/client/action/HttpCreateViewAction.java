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
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.GetViewAction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the create view API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpCreateViewAction extends HttpAction {

    /** The create view action. */
    protected final CreateViewAction action;

    /**
     * Creates a new HTTP create view action.
     *
     * @param client the HTTP client
     * @param action the create view action
     */
    public HttpCreateViewAction(final HttpClient client, final CreateViewAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the create view request asynchronously.
     *
     * @param request the create view request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final CreateViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        final String source = buildRequestBody(request);
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetViewAction.Response getViewResponse = GetViewAction.Response.fromXContent(parser);
                listener.onResponse(getViewResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the JSON request body for the create view request.
     *
     * @param request the create view request
     * @return the JSON request body
     */
    protected String buildRequestBody(final CreateViewAction.Request request) {
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("name", request.getName());
            builder.field("description", request.getDescription());
            builder.startArray("targets");
            for (final CreateViewAction.Request.Target target : request.getTargets()) {
                builder.startObject();
                builder.field("index_pattern", target.getIndexPattern());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            builder.flush();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
    }

    /**
     * Builds the curl request for the create view API.
     *
     * @param request the create view request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final CreateViewAction.Request request) {
        // RestViewAction
        return client.getCurlRequest(POST, "/views");
    }
}
