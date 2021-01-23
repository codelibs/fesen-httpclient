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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpPutStoredScriptAction extends HttpAction {

    protected final PutStoredScriptAction action;

    public HttpPutStoredScriptAction(final HttpClient client, final PutStoredScriptAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final PutStoredScriptRequest request, final ActionListener<AcknowledgedResponse> listener) {
        String source = null;
        try {
            source = XContentHelper.convertToJson(request.content(), true, false, request.xContentType());
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a reqsuest.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse putStoredScriptResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(putStoredScriptResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final PutStoredScriptRequest request) {
        // RestPutStoredScriptAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_scripts/" + UrlUtils.encode(request.id()));
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
