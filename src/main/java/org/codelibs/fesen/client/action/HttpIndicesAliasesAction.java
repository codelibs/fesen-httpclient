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
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpIndicesAliasesAction extends HttpAction {

    protected final IndicesAliasesAction action;

    public HttpIndicesAliasesAction(final HttpClient client, final IndicesAliasesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesAliasesRequest request, final ActionListener<AcknowledgedResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startArray("actions")) {
            for (final AliasActions aliasAction : request.getAliasActions()) {
                builder.startObject().startObject(aliasAction.actionType().toString().toLowerCase());
                builder.array("indices", aliasAction.indices());
                builder.array("aliases", aliasAction.aliases());
                if (aliasAction.filter() != null) {
                    builder.field("filter", aliasAction.filter());
                }
                if (aliasAction.indexRouting() != null) {
                    builder.field("index_routing", aliasAction.indexRouting());
                }
                if (aliasAction.searchRouting() != null) {
                    builder.field("search_routing", aliasAction.searchRouting());
                }
                builder.endObject().endObject();
            }
            builder.endArray().endObject();
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse indicesAliasesResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(indicesAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final IndicesAliasesRequest request) {
        // RestIndicesAliasesAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_aliases");
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
