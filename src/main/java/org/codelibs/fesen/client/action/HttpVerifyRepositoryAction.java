/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpVerifyRepositoryAction extends HttpAction {

    protected final VerifyRepositoryAction action;

    public HttpVerifyRepositoryAction(final HttpClient client, final VerifyRepositoryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final VerifyRepositoryRequest request, final ActionListener<VerifyRepositoryResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final VerifyRepositoryResponse verifyRepositoryResponse = VerifyRepositoryResponse.fromXContent(parser);
                listener.onResponse(verifyRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final VerifyRepositoryRequest request) {
        // RestVerifyRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_snapshot/" + UrlUtils.encode(request.name()) + "/_verify");
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
