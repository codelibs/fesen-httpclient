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
import java.util.HashMap;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;

/**
 * Handles the Get Mappings API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpGetMappingsAction extends HttpAction {

    /** The get mappings action definition. */
    protected final GetMappingsAction action;

    private static final ParseField MAPPINGS = new ParseField("mappings");

    /**
     * Creates a new HTTP get mappings action.
     *
     * @param client the HTTP client used to send requests
     * @param action the get mappings action definition
     */
    public HttpGetMappingsAction(final HttpClient client, final GetMappingsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get mappings request asynchronously.
     *
     * @param request the get mappings request
     * @param listener the listener notified with the get mappings response or a failure
     */
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

    /**
     * Builds the curl request for the get mappings API.
     *
     * @param request the get mappings request
     * @return the curl request to send
     */
    protected CurlRequest getCurlRequest(final GetMappingsRequest request) {
        // RestGetMappingAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mapping", request.indices());
        if (client.getEngineInfo().getType() != EngineType.ELASTICSEARCH8) {
            curlRequest.param("local", Boolean.toString(request.local()));
        }
        appendIndicesOptions(curlRequest, request.indicesOptions());
        return curlRequest;
    }

    // GetMappingsResponse does not provide fromXContent.
    // This custom implementation skips dynamic_templates for compatibility with older versions.
    /**
     * Parses the HTTP response body into a {@link GetMappingsResponse}.
     * Skips dynamic_templates entries for compatibility with older versions.
     *
     * @param parser the content parser for the response body
     * @return the parsed get mappings response
     * @throws IOException if parsing fails
     */
    public static GetMappingsResponse fromXContent(final XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        final Map<String, Object> parts = parser.map();

        Map<String, MappingMetadata> mappings = new HashMap<>();
        for (final Map.Entry<String, Object> entry : parts.entrySet()) {
            entry.getKey();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get(MAPPINGS.getPreferredName());

            final Map<String, MappingMetadata> typeBuilder = new HashMap<>();
            for (final Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                final String typeName = typeEntry.getKey();
                if ("dynamic_templates".equals(typeName)) {
                    continue;
                }
                final Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                final MappingMetadata mmd = new MappingMetadata(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            mappings = typeBuilder;
        }

        return new GetMappingsResponse(mappings);
    }

}
