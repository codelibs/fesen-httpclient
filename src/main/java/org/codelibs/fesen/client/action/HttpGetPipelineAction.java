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
import org.opensearch.action.ingest.GetPipelineAction;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.common.xcontent.XContentParser;

public class HttpGetPipelineAction extends HttpAction {

    protected final GetPipelineAction action;

    public HttpGetPipelineAction(final HttpClient client, final GetPipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetPipelineRequest request, final ActionListener<GetPipelineResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetPipelineResponse getPipelineResponse = GetPipelineResponse.fromXContent(parser);
                listener.onResponse(getPipelineResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetPipelineRequest request) {
        // RestGetPipelineAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_ingest/pipeline/" + UrlUtils.joinAndEncode(",", request.getIds()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
