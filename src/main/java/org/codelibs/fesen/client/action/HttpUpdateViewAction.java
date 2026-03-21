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
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.GetViewAction;
import org.opensearch.action.admin.indices.view.UpdateViewAction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpUpdateViewAction extends HttpAction {

    protected final UpdateViewAction action;

    public HttpUpdateViewAction(final HttpClient client, final UpdateViewAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CreateViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        String source = null;
        try (final XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
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
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetViewAction.Response getViewResponse = GetViewAction.Response.fromXContent(parser);
                listener.onResponse(getViewResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final CreateViewAction.Request request) {
        // RestViewAction
        return client.getCurlRequest(PUT, "/views/" + UrlUtils.encode(request.getName()));
    }
}
