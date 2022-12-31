/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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
import java.util.ArrayList;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.common.xcontent.XContentParser;

public class HttpGetIndexTemplatesAction extends HttpAction {

    protected final GetIndexTemplatesAction action;

    public HttpGetIndexTemplatesAction(final HttpClient client, final GetIndexTemplatesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIndexTemplatesResponse getIndexTemplatesResponse = fromXContent(parser);
                listener.onResponse(getIndexTemplatesResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetIndexTemplatesRequest request) {
        // RestGetIndexTemplateAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_template/" + UrlUtils.joinAndEncode(",", request.names()));
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

    private static GetIndexTemplatesResponse fromXContent(final XContentParser parser) throws IOException {
        final List<IndexTemplateMetadata> templates = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName());
                templates.add(templateMetadata);
            }
        }
        return new GetIndexTemplatesResponse(templates);
    }
}
