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

package org.codelibs.fesen.opensearch.action.admin.indices.template.put;

import org.codelibs.fesen.opensearch.OpenSearchGenerationException;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.codelibs.fesen.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.codelibs.fesen.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index template.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutIndexTemplateRequest extends ClusterManagerNodeRequest<PutIndexTemplateRequest>
    implements
        IndicesRequest,
        ToXContentObject {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(PutIndexTemplateRequest.class);

    private String name;

    private String cause = "";

    private List<String> indexPatterns;

    private int order;

    private boolean create;

    private Settings settings = EMPTY_SETTINGS;

    @Nullable
    private String mappings;

    private final Set<Alias> aliases = new HashSet<>();

    private Integer version;

    public PutIndexTemplateRequest() {}

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (indexPatterns == null || indexPatterns.size() == 0) {
            validationException = addValidationError("index patterns are missing", validationException);
        }
        return validationException;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    public PutIndexTemplateRequest patterns(List<String> indexPatterns) {
        this.indexPatterns = indexPatterns;
        return this;
    }

    public int order() {
        return this.order;
    }

    public boolean create() {
        return create;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    public String cause() {
        return this.cause;
    }

    @Override
    public String[] indices() {
        return indexPatterns.toArray(new String[0]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpand();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(name);
        out.writeStringCollection(indexPatterns);
        out.writeInt(order);
        out.writeBoolean(create);
        writeSettingsToStream(settings, out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeVInt(mappings == null ? 0 : 1);
            if (mappings != null) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
                out.writeString(mappings);
            }
        } else {
            out.writeOptionalString(mappings);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("index_patterns", indexPatterns);
            builder.field("order", order);
            if (version != null) {
                builder.field("version", version);
            }

            builder.startObject("settings");
            settings.toXContent(builder, params);
            builder.endObject();

            builder.startObject("mappings");
            if (mappings != null) {
                builder.field(MapperService.SINGLE_MAPPING_NAME);
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        mappings
                    )
                ) {
                    builder.copyCurrentStructure(parser);
                }
            }
            builder.endObject();
            builder.startObject("aliases");
            for (Alias alias : aliases) {
                alias.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
