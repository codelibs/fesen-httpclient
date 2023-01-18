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

import java.io.IOException;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.ParseField;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;

public class HttpGetMappingsAction extends HttpAction {

    protected final GetMappingsAction action;

    private static final ParseField MAPPINGS = new ParseField("mappings");

    public HttpGetMappingsAction(final HttpClient client, final GetMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            if (response.getHttpStatusCode() == 404) {
                throw new IndexNotFoundException(String.join(",", request.indices()));
            }
            try (final XContentParser parser = createParser(response)) {
                final GetMappingsResponse getMappingsResponse = /*GetMappingsResponse.*/fromXContent(parser);
                listener.onResponse(getMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetMappingsRequest request) {
        // RestGetMappingAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mapping", request.indices());
        if (client.getEngineInfo().getType() != EngineType.ELASTICSEARCH8) {
            curlRequest.param("local", Boolean.toString(request.local()));
        }
        return curlRequest;
    }

    // TODO replace with GetMappingsResonse#fromXContent, but it cannot parse dynamic_templates in 7.0.0-beta1.
    // from GetMappingsResponse
    public static GetMappingsResponse fromXContent(final XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        final Map<String, Object> parts = parser.map();

        ImmutableOpenMap<String, MappingMetadata> mappings = ImmutableOpenMap.of();
        for (final Map.Entry<String, Object> entry : parts.entrySet()) {
            entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get(MAPPINGS.getPreferredName());

            final ImmutableOpenMap.Builder<String, MappingMetadata> typeBuilder = new ImmutableOpenMap.Builder<>();
            for (final Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                final String typeName = typeEntry.getKey();
                assert typeEntry.getValue() instanceof Map
                        : "expected a map as inner type mapping, but got: " + typeEntry.getValue().getClass();
                if ("dynamic_templates".equals(typeName)) {
                    continue;
                }
                final Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                final MappingMetadata mmd = new MappingMetadata(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            mappings = typeBuilder.build();
        }

        return new GetMappingsResponse(mappings);
    }

}
