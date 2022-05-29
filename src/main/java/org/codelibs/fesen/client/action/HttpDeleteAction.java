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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.delete.DeleteResponse.Builder;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.VersionType;

public class HttpDeleteAction extends HttpAction {

    protected final DeleteAction action;

    public HttpDeleteAction(final HttpClient client, final DeleteAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeleteResponse deleteResponse = fromXContent(parser);
                listener.onResponse(deleteResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    // DeleteResponse.fromXContent(parser)
    protected DeleteResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        Builder context = new Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DeleteResponse.parseXContentFields(parser, context);
        }
        final EngineType engineType = client.getEngineInfo().getType();
        if (engineType == EngineType.ELASTICSEARCH8 || engineType == EngineType.OPENSEARCH2) {
            context.setType("_doc");
        }
        return context.build();
    }

    protected CurlRequest getCurlRequest(final DeleteRequest request) {
        // RestDeleteAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_doc/" + UrlUtils.encode(request.id()), request.index());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        if (!VersionType.INTERNAL.equals(request.versionType())) {
            curlRequest.param("version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        curlRequest.param("if_seq_no", Long.toString(request.ifSeqNo()));
        curlRequest.param("if_primary_term", Long.toString(request.ifPrimaryTerm()));
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        return curlRequest.param("routing", request.routing()).param("version", String.valueOf(request.version()));
    }
}
