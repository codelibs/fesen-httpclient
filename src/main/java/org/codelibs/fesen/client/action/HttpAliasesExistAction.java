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
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistAction;
import org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistResponse;
import org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesRequest;

public class HttpAliasesExistAction extends HttpAction {

    protected final AliasesExistAction action;

    public HttpAliasesExistAction(final HttpClient client, final AliasesExistAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetAliasesRequest request, final ActionListener<AliasesExistResponse> listener) {
        getCurlRequest(request).execute(response -> {
            boolean exists = false;
            switch (response.getHttpStatusCode()) {
            case 200:
                exists = true;
                break;
            case 404:
                exists = false;
                break;
            default:
                throw new FesenException("Unexpected status: " + response.getHttpStatusCode());
            }
            try {
                final AliasesExistResponse aliasesExistResponse = new AliasesExistResponse(exists);
                listener.onResponse(aliasesExistResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetAliasesRequest request) {
        // RestGetAliasesAction
        final CurlRequest curlRequest =
                client.getCurlRequest(HEAD, "/_alias/" + UrlUtils.joinAndEncode(",", request.aliases()), request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }
}
