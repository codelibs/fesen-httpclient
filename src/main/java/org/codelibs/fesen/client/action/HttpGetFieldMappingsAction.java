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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Get Field Mappings API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpGetFieldMappingsAction extends HttpAction {

    /** The get field mappings action definition. */
    protected final GetFieldMappingsAction action;

    /**
     * Creates a new HTTP get field mappings action.
     *
     * @param client the HTTP client used to send requests
     * @param action the get field mappings action definition
     */
    public HttpGetFieldMappingsAction(final HttpClient client, final GetFieldMappingsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the get field mappings request asynchronously.
     *
     * @param request the get field mappings request
     * @param listener the listener notified with the get field mappings response or a failure
     */
    public void execute(final GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetFieldMappingsResponse getFieldMappingsResponse = fromXContent(parser);
                listener.onResponse(getFieldMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the get field mappings API.
     *
     * @param request the get field mappings request
     * @return the curl request to send
     */
    protected CurlRequest getCurlRequest(final GetFieldMappingsRequest request) {
        // RestGetFieldMappingsAction
        final StringBuilder pathSuffix = new StringBuilder(100).append("/_mapping/field/");
        if (request.fields().length > 0) {
            pathSuffix.append(UrlUtils.joinAndEncode(",", request.fields()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathSuffix.toString(), request.indices());
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        if (client.getEngineInfo().getType() != EngineType.ELASTICSEARCH8) {
            curlRequest.param("local", Boolean.toString(request.local()));
        }
        return curlRequest;
    }

    /**
     * Parses the HTTP response body into a {@link GetFieldMappingsResponse}.
     *
     * @param parser the content parser for the response body
     * @return the parsed get field mappings response
     * @throws IOException if parsing fails
     */
    protected GetFieldMappingsResponse fromXContent(final XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();
                final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> typeMappings =
                        parseTypeMappings(parser, index);
                mappings.put(index, typeMappings);

                parser.nextToken();
            }
        }

        return newGetFieldMappingsResponse(mappings);
    }

    /**
     * Parses the type-level mappings of an index entry.
     *
     * @param parser the content parser positioned at an index entry
     * @param index the index name being parsed
     * @return a map of type name to field mapping metadata
     * @throws IOException if parsing fails
     */
    protected Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> parseTypeMappings(final XContentParser parser,
            final String index) throws IOException {
        final ObjectParser<Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>, String> objectParser =
                new ObjectParser<>(MAPPINGS_FIELD.getPreferredName(), true, HashMap::new);
        objectParser.declareField((p, typeMappings, idx) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, GetFieldMappingsResponse.FieldMappingMetadata> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);
                    do {
                        final String fieldName = p.currentName();
                        final GetFieldMappingsResponse.FieldMappingMetadata fieldMappingMetaData = getFieldMappingMetadata(p);
                        typeMapping.put(fieldName, fieldMappingMetaData);
                    } while (p.nextToken() == XContentParser.Token.START_OBJECT);
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS_FIELD, ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, index);
    }

    /**
     * Parses a single field mapping metadata entry.
     *
     * @param parser the content parser positioned at a field mapping object
     * @return the parsed field mapping metadata
     * @throws IOException if parsing fails
     */
    protected GetFieldMappingsResponse.FieldMappingMetadata getFieldMappingMetadata(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<GetFieldMappingsResponse.FieldMappingMetadata, String> objectParser =
                new ConstructingObjectParser<>("field_mapping_meta_data", true,
                        a -> new GetFieldMappingsResponse.FieldMappingMetadata((String) a[0], (BytesReference) a[1]));

        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME_FIELD,
                ObjectParser.ValueType.STRING);
        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> BytesReference.bytes(XContentFactory.jsonBuilder().copyCurrentStructure(p)), MAPPING_FIELD,
                ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, null);
    }

    /**
     * Creates a {@link GetFieldMappingsResponse} from the parsed mappings via reflection,
     * because the constructor is not publicly accessible.
     *
     * @param mappings the parsed mappings keyed by index, type, and field name
     * @return a new get field mappings response
     */
    protected GetFieldMappingsResponse newGetFieldMappingsResponse(
            final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappings) {
        final Class<GetFieldMappingsResponse> clazz = GetFieldMappingsResponse.class;
        final Class<?>[] types = { Map.class };
        try {
            final Constructor<GetFieldMappingsResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(mappings);
        } catch (final Exception e) {
            throw new OpenSearchException("Failed to create GetFieldMappingsResponse.", e);
        }
    }
}
