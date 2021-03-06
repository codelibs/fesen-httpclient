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
import org.codelibs.fesen.action.ingest.SimulatePipelineAction;
import org.codelibs.fesen.action.ingest.SimulatePipelineRequest;
import org.codelibs.fesen.action.ingest.SimulatePipelineResponse;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.json.JsonXContent;

public class HttpSimulatePipelineAction extends HttpAction {

    protected final SimulatePipelineAction action;

    public HttpSimulatePipelineAction(final HttpClient client, final SimulatePipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SimulatePipelineRequest request, final ActionListener<SimulatePipelineResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SimulatePipelineResponse cancelTasksResponse = SimulatePipelineResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SimulatePipelineRequest request) {
        // RestSimulatePipelineAction
        final String path = request.getId() != null ? "/_ingest/pipeline/" + UrlUtils.encode(request.getId()) + "/_simulate"
                : "/_ingest/pipeline/_simulate";
        final CurlRequest curlRequest = client.getCurlRequest(POST, path);
        curlRequest.param("verbose", String.valueOf(request.isVerbose()));
        return curlRequest;
    }
}
