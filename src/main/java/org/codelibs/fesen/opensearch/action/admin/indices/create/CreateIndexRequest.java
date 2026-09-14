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

package org.codelibs.fesen.opensearch.action.admin.indices.create;

import org.codelibs.fesen.opensearch.OpenSearchGenerationException;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.cluster.metadata.Context;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;
import org.codelibs.fesen.opensearch.transport.client.IndicesAdminClient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.codelibs.fesen.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.codelibs.fesen.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index. Best created with {@code Requests#createIndexRequest(String)}.
 * <p>
 * The index created can optionally be created with {@link #settings(org.codelibs.fesen.opensearch.common.settings.Settings)}.
 *
 * @see IndicesAdminClient#create(CreateIndexRequest)
 * @see CreateIndexResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    /**
     * The MAPPINGS constant.
     */
    public static final ParseField MAPPINGS = new ParseField("mappings");
    /**
     * The SETTINGS constant.
     */
    public static final ParseField SETTINGS = new ParseField("settings");
    /**
     * The ALIASES constant.
     */
    public static final ParseField ALIASES = new ParseField("aliases");
    /**
     * The CONTEXT constant.
     */
    public static final ParseField CONTEXT = new ParseField("context");

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private String mappings = "{}";

    private final Set<Alias> aliases = new HashSet<>();

    private Context context;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    /**
     * Creates a new CreateIndexRequest.
     */
    public CreateIndexRequest() {}

    /**
     * Constructs a new request to create an index with the specified name.
     *
     * @param index the index
     */
    public CreateIndexRequest(String index) {
        this(index, EMPTY_SETTINGS);
    }

    /**
     * Constructs a new request to create an index with the specified name and settings.
     *
     * @param index the index
     * @param settings the settings
     */
    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    /**
     * The index name to create.
     *
     * @return this instance
     */
    public String index() {
        return index;
    }

    /**
     * Indexes this instance.
     *
     * @param index the index
     * @return this instance
     */
    public CreateIndexRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * The settings to create the index with.
     *
     * @return the settings
     */
    public Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     *
     * @return the cause
     */
    public String cause() {
        return cause;
    }

    /**
     * The settings to create the index with.
     *
     * @param settings the settings
     * @return the settings
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index with.
     *
     * @param settings the settings
     * @return the settings
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index with (either json or yaml format)
     *
     * @param source the source
     * @param xContentType the content type
     * @return the settings
     * @deprecated use {@link #settings(String source, MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * The settings to create the index with (using a generic MediaType)
     *
     * @param source the source
     * @param mediaType the media type
     * @return the settings
     */
    public CreateIndexRequest settings(String source, MediaType mediaType) {
        this.settings = Settings.builder().loadFromSource(source, mediaType).build();
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     *
     * @param builder the content builder
     * @return the settings
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        settings(builder.toString(), builder.contentType());
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     *
     * @param source the source
     * @return the settings
     */
    public CreateIndexRequest settings(Map<String, ?> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Set the mapping for this index
     * <p>
     * The mapping should be in the form of a JSON string, with an outer _doc key
     * <pre>
     *     .mapping("{\"_doc\":{\"properties\": ... }}")
     * </pre>
     *
     * @param mapping the mapping
     * @return the mapping
     */
    public CreateIndexRequest mapping(String mapping) {
        this.mappings = mapping;
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType The content type of the source
     *
     * @return the mapping
     * @deprecated use {@link #mapping(String source, MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest mapping(String source, XContentType xContentType) {
        return mapping(new BytesArray(source), xContentType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     * <p>
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param mediaType The media type of the source
     * @return the mapping
     */
    public CreateIndexRequest mapping(String source, MediaType mediaType) {
        return mapping(new BytesArray(source), mediaType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType the content type of the mapping source
     *
     * @deprecated use {@link #mapping(BytesReference source, MediaType mediaType)} instead
     */
    @Deprecated
    private CreateIndexRequest mapping(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        return mapping(MapperService.SINGLE_MAPPING_NAME, mappingAsMap);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     * <p>
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param mediaType The media type of the source
     * @return the mapping
     */
    public CreateIndexRequest mapping(BytesReference source, MediaType mediaType) {
        Objects.requireNonNull(mediaType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, mediaType).v2();
        return mapping(MapperService.SINGLE_MAPPING_NAME, mappingAsMap);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @return the mapping
     */
    public CreateIndexRequest mapping(XContentBuilder source) {
        return mapping(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Set the mapping for this index
     *
     * @param source The mapping source
     * @return the mapping
     */
    public CreateIndexRequest mapping(Map<String, ?> source) {
        return mapping(MapperService.SINGLE_MAPPING_NAME, source);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @deprecated types are being removed
     */
    @Deprecated
    private CreateIndexRequest mapping(String type, Map<String, ?> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, source);
        } else if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
            // if it has a different type name, then unwrap and rewrap with _doc
            source = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, source.get(type));
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return mapping(builder.toString());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     *
     * @param source the source
     * @return the simple mapping
     */
    public CreateIndexRequest simpleMapping(String... source) {
        mapping(PutMappingRequest.simpleMapping(source));
        return this;
    }

    /**
     * The cause for this index creation.
     *
     * @param cause the cause
     * @return the cause
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     *
     * @param source the source
     * @return the aliases
     */
    public CreateIndexRequest aliases(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     *
     * @param source the source
     * @return the aliases
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     *
     * @param source the source
     * @return the aliases
     */
    public CreateIndexRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     *
     * @param source the source
     * @return the aliases
     */
    public CreateIndexRequest aliases(BytesReference source) {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
            // move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse aliases", e);
        }
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     *
     * @param alias the alias
     * @return the alias
     */
    public CreateIndexRequest alias(Alias alias) {
        this.aliases.add(alias);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param xContentType the content type
     * @return the source
     * @deprecated use {@link #source(String, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     * <p>
     * Note that the mapping definition should *not* be nested under a type name.
     *
     * @param source the source
     * @param mediaType the media type
     * @return the source
     */
    public CreateIndexRequest source(String source, MediaType mediaType) {
        return source(new BytesArray(source), mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @return the source
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param xContentType the content type
     * @return the source
     * @deprecated use {@link #source(byte[], MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     * <p>
     * Note that the mapping definition should *not* be nested under a type name.
     *
     * @param source the source
     * @param mediaType the media type
     * @return the source
     */
    public CreateIndexRequest source(byte[] source, MediaType mediaType) {
        return source(source, 0, source.length, mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param offset the offset
     * @param length the length
     * @param xContentType the content type
     * @return the source
     * @deprecated use {@link #source(byte[], int, int, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param offset the offset
     * @param length the length
     * @param mediaType the media type
     * @return the source
     */
    public CreateIndexRequest source(byte[] source, int offset, int length, MediaType mediaType) {
        return source(new BytesArray(source, offset, length), mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param xContentType the content type
     * @return the source
     * @deprecated use {@link #source(BytesReference, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        source(XContentHelper.convertToMap(source, false, xContentType).v2(), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param mediaType the media type
     * @return the source
     */
    public CreateIndexRequest source(BytesReference source, MediaType mediaType) {
        Objects.requireNonNull(mediaType);
        source(XContentHelper.convertToMap(source, false, mediaType).v2(), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @param source the source
     * @param deprecationHandler the deprecation handler
     * @return the source
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, ?> source, DeprecationHandler deprecationHandler) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            String name = entry.getKey();
            if (SETTINGS.match(name, deprecationHandler)) {
                if (entry.getValue() instanceof Map == false) {
                    throw new OpenSearchParseException("key [settings] must be an object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (MAPPINGS.match(name, deprecationHandler)) {
                if (entry.getValue() instanceof Map == false) {
                    throw new OpenSearchParseException("key [mappings] must be an object");
                }
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else if (ALIASES.match(name, deprecationHandler)) {
                aliases((Map<String, Object>) entry.getValue());
            } else if (CONTEXT.match(name, deprecationHandler)) {
                context((Map<String, Object>) entry.getValue());
            } else {
                throw new OpenSearchParseException("unknown key [{}] for create index", name);
            }
        }
        return this;
    }

    /**
     * Returns the mappings.
     *
     * @return the mappings
     */
    public String mappings() {
        return this.mappings;
    }

    /**
     * Returns the aliases.
     *
     * @return the aliases
     */
    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Waits the for active shards.
     *
     * @return this instance
     */
    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active for index creation to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link CreateIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     * @return this instance
     */
    public CreateIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     *
     * @param waitForActiveShards the wait for active shards
     * @return this instance
     */
    public CreateIndexRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Returns the context.
     *
     * @param source the source
     * @return the context
     */
    public CreateIndexRequest context(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return context(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Returns the context.
     *
     * @param source the source
     * @return the context
     */
    public CreateIndexRequest context(BytesReference source) {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
            // move to the first alias
            context(Context.fromXContent(parser));
            return this;
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse context", e);
        }
    }

    /**
     * Returns the context.
     *
     * @param context the context
     * @return the context
     */
    public CreateIndexRequest context(Context context) {
        this.context = context;
        return this;
    }

    /**
     * Returns the context.
     *
     * @return the context
     */
    public Context context() {
        return context;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(settings, out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            if ("{}".equals(mappings)) {
                out.writeVInt(0);
            } else {
                out.writeVInt(1);
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
                out.writeString(mappings);
            }
        } else {
            out.writeString(mappings);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        waitForActiveShards.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeOptionalWriteable(context);
        }
    }

    @Override
    public String toString() {
        return "CreateIndexRequest{"
            + "cause='"
            + cause
            + '\''
            + ", index='"
            + index
            + '\''
            + ", settings="
            + settings
            + ", mappings='"
            + mappings
            + '\''
            + ", aliases="
            + aliases
            + '\''
            + ", context="
            + context
            + ", waitForActiveShards="
            + waitForActiveShards
            + '}';
    }
}
