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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.core.xcontent.XContentParser;

public class HttpForceMergeAction extends HttpAction {

    protected final ForceMergeAction action;

    public HttpForceMergeAction(final HttpClient client, final ForceMergeAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ForceMergeRequest request, final ActionListener<ForceMergeResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ForceMergeResponse forceMergeResponse = ForceMergeResponse.fromXContent(parser);
                listener.onResponse(forceMergeResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ForceMergeRequest request) {
        // RestForceMergeAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_forcemerge", request.indices());
        return curlRequest.param("max_num_segments", String.valueOf(request.maxNumSegments()))
                .param("only_expunge_deletes", String.valueOf(request.onlyExpungeDeletes()))
                .param("flush", String.valueOf(request.flush()));
    }
}
