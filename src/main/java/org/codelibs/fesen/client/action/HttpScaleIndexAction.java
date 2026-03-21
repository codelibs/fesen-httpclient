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

import java.lang.reflect.Method;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.scale.searchonly.ScaleIndexAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpScaleIndexAction extends HttpAction {

    protected final ScaleIndexAction action;

    public HttpScaleIndexAction(final HttpClient client, final ScaleIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ActionRequest request, final ActionListener<AcknowledgedResponse> listener) {
        // ScaleIndexRequest is package-private in OpenSearch, so reflection is the only way
        // to access getIndex() and isScaleDown() from outside the package.
        try {
            final Method getIndexMethod = request.getClass().getMethod("getIndex");
            getIndexMethod.setAccessible(true);
            final Method isScaleDownMethod = request.getClass().getMethod("isScaleDown");
            isScaleDownMethod.setAccessible(true);
            final String index = (String) getIndexMethod.invoke(request);
            final boolean scaleDown = (boolean) isScaleDownMethod.invoke(request);

            final String body = "{\"search_only\":" + scaleDown + "}";
            final CurlRequest curlRequest = client.getCurlRequest(POST, "/_scale", index);
            curlRequest.body(body).execute(response -> {
                try (final XContentParser parser = createParser(response)) {
                    final AcknowledgedResponse ackResponse = AcknowledgedResponse.fromXContent(parser);
                    listener.onResponse(ackResponse);
                } catch (final Exception e) {
                    listener.onFailure(toOpenSearchException(response, e));
                }
            }, e -> unwrapOpenSearchException(listener, e));
        } catch (final Exception e) {
            listener.onFailure(new OpenSearchException("Failed to execute scale index action", e));
        }
    }
}
