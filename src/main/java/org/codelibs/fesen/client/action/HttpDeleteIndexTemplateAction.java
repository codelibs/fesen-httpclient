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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.core.xcontent.XContentParser;

public class HttpDeleteIndexTemplateAction extends HttpAction {

    protected final DeleteIndexTemplateAction action;

    public HttpDeleteIndexTemplateAction(final HttpClient client, final DeleteIndexTemplateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse deleteIndexTemplateResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(deleteIndexTemplateResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final DeleteIndexTemplateRequest request) {
        // RestDeleteIndexTemplatesAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_template/" + UrlUtils.encode(request.name()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
