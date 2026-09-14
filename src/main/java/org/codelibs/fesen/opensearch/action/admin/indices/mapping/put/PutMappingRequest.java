/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.mapping.put;

import org.codelibs.fesen.opensearch.OpenSearchGenerationException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;
import org.codelibs.fesen.opensearch.transport.client.IndicesAdminClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Puts mapping definition into one or more indices. Best created with
 * {@code Requests#putMappingRequest(String...)}.
 * <p>
 * If the mappings already exists, the new mappings will be merged with the new one. If there are elements
 * that can't be merged are detected, the request will be rejected.
 *
 * @see IndicesAdminClient#putMapping(PutMappingRequest)
 * @see AcknowledgedResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutMappingRequest extends AcknowledgedRequest<PutMappingRequest> implements IndicesRequest.Replaceable, ToXContentObject {

    private static final Set<String> RESERVED_FIELDS = Set.of(
        "_uid",
        "_id",
        "_type",
        "_source",
        "_all",
        "_analyzer",
        "_parent",
        "_routing",
        "_index",
        "_size",
        "_timestamp",
        "_ttl",
        "_field_names"
    );

    private String[] indices;

    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);

    private String source;
    private String origin = "";

    private Index concreteIndex;

    private boolean writeIndexOnly;

    /**
     * Creates a new PutMappingRequest.
     */
    public PutMappingRequest() {}

    /**
     * Constructs a new put mapping request against one or more indices. If nothing is set then
     * it will be executed against all indices.
     *
     * @param indices the indices
     */
    public PutMappingRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("mapping source is missing", validationException);
        } else if (source.isEmpty()) {
            validationException = addValidationError("mapping source is empty", validationException);
        }
        if (concreteIndex != null && CollectionUtils.isEmpty(indices) == false) {
            validationException = addValidationError(
                "either concrete index or unresolved indices can be set, concrete index: ["
                    + concreteIndex
                    + "] and indices: "
                    + Arrays.asList(indices),
                validationException
            );
        }
        return validationException;
    }

    /**
     * Sets the indices this put mapping operation will execute on.
     */
    @Override
    public PutMappingRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The indices the mappings will be put.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * The mapping source definition.
     *
     * @return the source
     */
    public String source() {
        return source;
    }

    /**
     * Returns the simple mapping.
     *
     * @param source
     *            consisting of field/properties pairs (e.g. "field1",
     *            "type=string,store=true")
     * @throws IllegalArgumentException
     *             if the number of the source arguments is not divisible by two
     * @return the mappings definition
     */
    public static XContentBuilder simpleMapping(String... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("mapping source must be pairs of fieldnames and properties definition.");
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();

            for (int i = 0; i < source.length; i++) {
                String fieldName = source[i++];
                if (RESERVED_FIELDS.contains(fieldName)) {
                    builder.startObject(fieldName);
                    String[] s1 = Strings.splitStringByCommaToArray(source[i]);
                    for (String s : s1) {
                        String[] s2 = Strings.split(s, "=");
                        if (s2.length != 2) {
                            throw new IllegalArgumentException("malformed " + s);
                        }
                        builder.field(s2[0], s2[1]);
                    }
                    builder.endObject();
                }
            }

            builder.startObject("properties");
            for (int i = 0; i < source.length; i++) {
                String fieldName = source[i++];
                if (RESERVED_FIELDS.contains(fieldName)) {
                    continue;
                }

                builder.startObject(fieldName);
                String[] s1 = Strings.splitStringByCommaToArray(source[i]);
                for (String s : s1) {
                    String[] s2 = Strings.split(s, "=");
                    if (s2.length != 2) {
                        throw new IllegalArgumentException("malformed " + s);
                    }
                    builder.field(s2[0], s2[1]);
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return builder;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to generate simplified mapping definition", e);
        }
    }

    /**
     * The mapping source definition.
     *
     * @param mappingBuilder the mapping builder
     * @return the source
     */
    public PutMappingRequest source(XContentBuilder mappingBuilder) {
        return source(BytesReference.bytes(mappingBuilder), mappingBuilder.contentType());
    }

    /**
     * The mapping source definition.
     *
     * @param mappingSource the mapping source
     * @param mediaType the media type
     * @return the source
     */
    public PutMappingRequest source(String mappingSource, MediaType mediaType) {
        return source(new BytesArray(mappingSource), mediaType);
    }

    /**
     * The mapping source definition.
     *
     * @param mappingSource the mapping source
     * @param mediaType the media type
     * @return the source
     */
    public PutMappingRequest source(BytesReference mappingSource, MediaType mediaType) {
        Objects.requireNonNull(mediaType);
        try {
            this.source = XContentHelper.convertToJson(mappingSource, false, false, mediaType);
            return this;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to convert source to json", e);
        }
    }

    /**
     * Writes the index only.
     *
     * @param writeIndexOnly the write index only
     * @return this instance
     */
    public PutMappingRequest writeIndexOnly(boolean writeIndexOnly) {
        this.writeIndexOnly = writeIndexOnly;
        return this;
    }

    /**
     * Writes the index only.
     *
     * @return this instance
     */
    public boolean writeIndexOnly() {
        return writeIndexOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(source);
        out.writeOptionalWriteable(concreteIndex);
        out.writeOptionalString(origin);
        out.writeBoolean(writeIndexOnly);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source != null) {
            try (InputStream stream = new BytesArray(source).streamInput()) {
                builder.rawValue(stream, MediaTypeRegistry.JSON);
            }
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
