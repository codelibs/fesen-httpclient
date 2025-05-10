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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.alias.get.GetAliasesAction;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

public class HttpGetAliasesAction extends HttpAction {

    protected final GetAliasesAction action;

    public HttpGetAliasesAction(final HttpClient client, final GetAliasesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetAliasesRequest request, final ActionListener<GetAliasesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetAliasesResponse getAliasesResponse = getGetAliasesResponse(parser);
                listener.onResponse(getAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetAliasesRequest request) {
        // RestGetAliasesAction
        final CurlRequest curlRequest =
                client.getCurlRequest(GET, "/_alias/" + UrlUtils.joinAndEncode(",", request.aliases()), request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }

    protected GetAliasesResponse getGetAliasesResponse(final XContentParser parser) throws IOException {
        final Map<String, List<AliasMetadata>> aliases = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String index = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                index = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliases.put(index, getAliases(parser));
                    } else {
                        parser.skipChildren();
                    }
                }
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(aliases.size());
            for (final Map.Entry<String, List<AliasMetadata>> entry : aliases.entrySet()) {
                out.writeString(entry.getKey());
                out.writeVInt(entry.getValue().size());
                for (final AliasMetadata aliasMetaData : entry.getValue()) {
                    aliasMetaData.writeTo(out);
                }
            }
            return action.getResponseReader().read(out.toStreamInput());
        }
    }

    public static List<AliasMetadata> getAliases(final XContentParser parser) throws IOException {
        final List<AliasMetadata> aliases = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            return aliases;
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                aliases.add(AliasMetadata.Builder.fromXContent(parser));
            }
        }
        return aliases;
    }
}
