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

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.codelibs.fesen.opensearch.action.AliasesRequest;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.ClusterState.FeatureAware;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.cluster.Diffable;
import org.codelibs.fesen.opensearch.cluster.DiffableUtils;
import org.codelibs.fesen.opensearch.cluster.NamedDiffable;
import org.codelibs.fesen.opensearch.cluster.NamedDiffableValueSerializer;
import org.codelibs.fesen.opensearch.cluster.applicationtemplates.SystemTemplateMetadata;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlock;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.coordination.CoordinationMetadata;
import org.codelibs.fesen.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingPool;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.UUIDs;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.IndexNotFoundException;
import org.codelibs.fesen.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.codelibs.fesen.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.codelibs.fesen.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * Metadata information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata>, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(Metadata.class);

    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = Strings.UNKNOWN_UUID_VALUE;
    public static final Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+$");

    /**
     * Context of the XContent.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT
    }

    /**
     * Indicates that this custom metadata will be returned as part of an API call but will not be persisted
     */
    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and stored as a part of
     * a snapshot global state, but will not be persisted between node restarts
     */
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    /**
     * Indicates that this custom metadata will be returned as part of an API call, stored as a part of
     * a snapshot global state, and will be persisted between node restarts
     */
    public static EnumSet<XContentContext> ALL_CONTEXTS = EnumSet.allOf(XContentContext.class);

    /**
     * Custom metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment, ClusterState.FeatureAware {

        EnumSet<XContentContext> context();
    }

    public static final Setting<Integer> DEFAULT_REPLICA_COUNT_SETTING = Setting.intSetting(
        "cluster.default_number_of_replicas",
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(
        6,
        "cluster read-only (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final ClusterBlock CLUSTER_CREATE_INDEX_BLOCK = new ClusterBlock(
        10,
        "cluster create-index blocked (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.CREATE_INDEX)
    );

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only_allow_delete",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_CREATE_INDEX_BLOCK_SETTING = Setting.boolSetting(
        "cluster.blocks.create_index",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(
        13,
        "cluster read-only / allow delete (api)",
        false,
        false,
        true,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String CONTEXT_MODE_API = XContentContext.API.toString();

    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    public static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    private final Map<String, IndexMetadata> indices;
    private final TemplatesMetadata templates;
    private final Map<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenLocalOnlyIndexShards;
    private final int totalOpenRemoteCapableIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private final SortedMap<String, IndexAbstraction> indicesLookup;

    private final Map<String, SortedMap<Long, String>> systemTemplatesLookup;

    Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        DiffableStringMap hashesOfConsistentSettings,
        final Map<String, IndexMetadata> indices,
        final Map<String, IndexTemplateMetadata> templates,
        final Map<String, Custom> customs,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, SortedMap<Long, String>> systemTemplatesLookup
    ) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.indices = Collections.unmodifiableMap(indices);
        this.customs = Collections.unmodifiableMap(customs);
        this.templates = new TemplatesMetadata(templates);
        int totalNumberOfShards = 0;
        int totalOpenLocalOnlyIndexShards = 0;
        int totalOpenRemoteCapableIndexShards = 0;
        for (IndexMetadata cursor : indices.values()) {
            totalNumberOfShards += cursor.getTotalNumberOfShards();
            if (IndexMetadata.State.OPEN.equals(cursor.getState())) {
                if (RoutingPool.getIndexPool(cursor) == RoutingPool.REMOTE_CAPABLE) {
                    totalOpenRemoteCapableIndexShards += cursor.getTotalNumberOfShards();
                } else {
                    totalOpenLocalOnlyIndexShards += cursor.getTotalNumberOfShards();
                }
            }
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenLocalOnlyIndexShards = totalOpenLocalOnlyIndexShards;
        this.totalOpenRemoteCapableIndexShards = totalOpenRemoteCapableIndexShards;

        this.allIndices = allIndices;
        this.visibleIndices = visibleIndices;
        this.allOpenIndices = allOpenIndices;
        this.visibleOpenIndices = visibleOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.visibleClosedIndices = visibleClosedIndices;
        this.indicesLookup = indicesLookup;
        this.systemTemplatesLookup = systemTemplatesLookup;
    }

    public long version() {
        return this.version;
    }

    public String clusterUUID() {
        return this.clusterUUID;
    }

    /**
     * Whether the current node with the given cluster state is locked into the cluster with the UUID returned by {@link #clusterUUID()},
     * meaning that it will not accept any cluster state with a different clusterUUID.
     */
    public boolean clusterUUIDCommitted() {
        return this.clusterUUIDCommitted;
    }

    public Settings transientSettings() {
        return this.transientSettings;
    }

    public Settings persistentSettings() {
        return this.persistentSettings;
    }

    public Map<String, String> hashesOfConsistentSettings() {
        return this.hashesOfConsistentSettings;
    }

    public CoordinationMetadata coordinationMetadata() {
        return this.coordinationMetadata;
    }

    private static String mergePaths(String path, String field) {
        if (path.length() == 0) {
            return field;
        }
        return path + "." + field;
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] getConcreteAllIndices() {
        return allIndices;
    }

    /**
     * Returns all the concrete indices that are not hidden.
     */
    public String[] getConcreteVisibleIndices() {
        return visibleIndices;
    }

    /**
     * Returns all of the concrete indices that are open.
     */
    public String[] getConcreteAllOpenIndices() {
        return allOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are open and not hidden.
     */
    public String[] getConcreteVisibleOpenIndices() {
        return visibleOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are closed.
     */
    public String[] getConcreteAllClosedIndices() {
        return allClosedIndices;
    }

    /**
     * Returns all of the concrete indices that are closed and not hidden.
     */
    public String[] getConcreteVisibleClosedIndices() {
        return visibleClosedIndices;
    }

    /**
     * Returns indexing routing for the given index.
     */
    // TODO: This can be moved to IndexNameExpressionResolver too, but this means that we will support wildcards and other expressions

    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    public IndexMetadata index(String index) {
        return indices.get(index);
    }

    public IndexMetadata index(Index index) {
        IndexMetadata metadata = index(index.getName());
        if (metadata != null && metadata.getIndexUUID().equals(index.getUUID())) {
            return metadata;
        }
        return null;
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return this.templates.getTemplates();
    }

    public Map<String, IndexTemplateMetadata> getTemplates() {
        return templates();
    }

    public TemplatesMetadata templatesMetadata() {
        return this.templates;
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return Optional.ofNullable((ComponentTemplateMetadata) this.custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, SortedMap<Long, String>> systemTemplatesLookup() {
        return systemTemplatesLookup;
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) this.custom(ComposableIndexTemplateMetadata.TYPE))
            .map(ComposableIndexTemplateMetadata::indexTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, DataStream> dataStreams() {
        return Optional.ofNullable((DataStreamMetadata) this.custom(DataStreamMetadata.TYPE))
            .map(DataStreamMetadata::dataStreams)
            .orElse(Collections.emptyMap());
    }

    public Map<String, View> views() {
        return Optional.ofNullable((ViewMetadata) this.custom(ViewMetadata.TYPE)).map(ViewMetadata::views).orElse(Collections.emptyMap());
    }

    public Map<String, WorkloadGroup> workloadGroups() {
        return Optional.ofNullable((WorkloadGroupMetadata) this.custom(WorkloadGroupMetadata.TYPE))
            .map(WorkloadGroupMetadata::workloadGroups)
            .orElse(Collections.emptyMap());
    }

    public DecommissionAttributeMetadata decommissionAttributeMetadata() {
        return custom(DecommissionAttributeMetadata.TYPE);
    }

    public Map<String, Custom> customs() {
        return this.customs;
    }

    public Map<String, Custom> getCustoms() {
        return this.customs();
    }

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return custom(IndexGraveyard.TYPE);
    }

    /**
     * *
     * @return The weighted routing metadata for search requests
     */
    public WeightedRoutingMetadata weightedRoutingMetadata() {
        return custom(WeightedRoutingMetadata.TYPE);
    }

    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenLocalOnlyIndexShards;
    }

    /**
     * Gets the total number of open remote capable shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenRemoteCapableIndexShards() {
        return this.totalOpenRemoteCapableIndexShards;
    }

    /**
     * Identifies whether the array containing type names given as argument refers to all types
     * The empty or null array identifies all types
     *
     * @param types the array containing types
     * @return true if the provided array maps to all types, false otherwise
     */
    public static boolean isAllTypes(String[] types) {
        return types == null || types.length == 0 || isExplicitAllType(types);
    }

    /**
     * Identifies whether the array containing type names given as argument explicitly refers to all types
     * The empty or null array doesn't explicitly map to all types
     *
     * @param types the array containing index names
     * @return true if the provided array explicitly maps to all types, false otherwise
     */
    public static boolean isExplicitAllType(String[] types) {
        return types != null && types.length == 1 && ALL.equals(types[0]);
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.values().iterator();
    }

    public static boolean isTransientSettingsMetadataEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.transientSettings.equals(metadata2.transientSettings);
    }

    public static boolean isHashesOfConsistentSettingsEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.hashesOfConsistentSettings.equals(metadata2.hashesOfConsistentSettings);
    }

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return new MetadataDiff(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    /**
     * A diff of metadata.
     *
     * @opensearch.internal
     */
    private static class MetadataDiff implements Diff<Metadata> {

        private final long version;
        private final String clusterUUID;
        private boolean clusterUUIDCommitted;
        private final CoordinationMetadata coordinationMetadata;
        private final Settings transientSettings;
        private final Settings persistentSettings;
        private final Diff<DiffableStringMap> hashesOfConsistentSettings;
        private final Diff<Map<String, IndexMetadata>> indices;
        private final Diff<Map<String, IndexTemplateMetadata>> templates;
        private final Diff<Map<String, Custom>> customs;

        MetadataDiff(Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(
                before.templates.getTemplates(),
                after.templates.getTemplates(),
                DiffableUtils.getStringKeySerializer()
            );
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            indices = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            hashesOfConsistentSettings.writeTo(out);
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public Metadata apply(Metadata part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates.getTemplates()));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetadata.readFrom(in));
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        hashesOfConsistentSettings.writeTo(out);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out);
        }
        templates.writeTo(out);
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final Custom cursor : customs.values()) {
            if (FeatureAware.shouldSerialize(out, cursor)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final Custom cursor : customs.values()) {
            if (FeatureAware.shouldSerialize(out, cursor)) {
                out.writeNamedWriteable(cursor);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;
        private DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(Collections.emptyMap());

        private final Map<String, IndexMetadata> indices;
        private final Map<String, IndexTemplateMetadata> templates;
        private final Map<String, Custom> customs;
        private final Metadata previousMetadata;

        private Map<String, SortedMap<Long, String>> systemTemplatesLookup;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = new HashMap<>();
            templates = new HashMap<>();
            customs = new HashMap<>();
            previousMetadata = null;
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            if (indices.get(indexMetadata.getIndex().getName()) == indexMetadata) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetadata = IndexMetadata.builder(indexMetadata).version(indexMetadata.getVersion() + 1).build();
            }
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            indices.clear();
            return this;
        }

        public Builder indices(final Map<String, IndexMetadata> indices) {
            this.indices.putAll(indices);
            return this;
        }

        public Builder put(IndexTemplateMetadata.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            this.templates.putAll(templates);
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            Objects.requireNonNull(componentTemplate, "it is invalid to add a null component template: " + name);
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, componentTemplate);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(componentTemplates));
            return this;
        }

        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(indexTemplates));
            return this;
        }

        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            Objects.requireNonNull(indexTemplate, "it is invalid to add a null index template: " + name);
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, indexTemplate);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            return ((DataStreamMetadata) customs.get(DataStreamMetadata.TYPE)).dataStreams().get(dataStreamName);
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams) {
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(dataStreams));
            return this;
        }

        public Builder put(DataStream dataStream) {
            Objects.requireNonNull(dataStream, "it is invalid to add a null data stream");
            Map<String, DataStream> existingDataStreams = Optional.ofNullable(
                (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE)
            ).map(dsmd -> new HashMap<>(dsmd.dataStreams())).orElse(new HashMap<>());
            existingDataStreams.put(dataStream.getName(), dataStream);
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams));
            return this;
        }

        public Builder removeDataStream(String name) {
            Map<String, DataStream> existingDataStreams = Optional.ofNullable(
                (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE)
            ).map(dsmd -> new HashMap<>(dsmd.dataStreams())).orElse(new HashMap<>());
            existingDataStreams.remove(name);
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams));
            return this;
        }

        public Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(Map<String, Custom> customs) {
            StreamSupport.stream(Spliterators.spliterator(customs.entrySet(), 0), false)
                .forEach(cursor -> Objects.requireNonNull(cursor.getValue(), cursor.getKey()));
            this.customs.putAll(customs);
            return this;
        }

        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            putCustom(IndexGraveyard.TYPE, indexGraveyard);
            return this;
        }

        public IndexGraveyard indexGraveyard() {
            IndexGraveyard graveyard = (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
            return graveyard;
        }

        public Builder decommissionAttributeMetadata(final DecommissionAttributeMetadata decommissionAttributeMetadata) {
            putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
            return this;
        }

        public DecommissionAttributeMetadata decommissionAttributeMetadata() {
            return (DecommissionAttributeMetadata) getCustom(DecommissionAttributeMetadata.TYPE);
        }

        public Builder coordinationMetadata(CoordinationMetadata coordinationMetadata) {
            this.coordinationMetadata = coordinationMetadata;
            return this;
        }

        public Settings transientSettings() {
            return this.transientSettings;
        }

        public Builder transientSettings(Settings settings) {
            this.transientSettings = settings;
            return this;
        }

        public Settings persistentSettings() {
            return this.persistentSettings;
        }

        public Builder persistentSettings(Settings settings) {
            this.persistentSettings = settings;
            return this;
        }

        public DiffableStringMap hashesOfConsistentSettings() {
            return this.hashesOfConsistentSettings;
        }

        public Builder hashesOfConsistentSettings(DiffableStringMap hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = hashesOfConsistentSettings;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }

        public Builder generateClusterUuidIfNeeded() {
            if (clusterUUID.equals(UNKNOWN_CLUSTER_UUID)) {
                clusterUUID = UUIDs.randomBase64UUID();
            }
            return this;
        }

        public Metadata build() {
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            DataStreamMetadata previousDataStreamMetadata = (previousMetadata != null)
                ? (DataStreamMetadata) this.previousMetadata.customs.get(DataStreamMetadata.TYPE)
                : null;

            buildSystemTemplatesLookup();

            boolean recomputeRequiredforIndicesLookups = (previousMetadata == null)
                || (indices.equals(previousMetadata.indices) == false)
                || (previousDataStreamMetadata != null && previousDataStreamMetadata.equals(dataStreamMetadata) == false)
                || (dataStreamMetadata != null && dataStreamMetadata.equals(previousDataStreamMetadata) == false);

            return (recomputeRequiredforIndicesLookups == false)
                ? buildMetadataWithPreviousIndicesLookups()
                : buildMetadataWithRecomputedIndicesLookups();
        }

        private void buildSystemTemplatesLookup() {
            if (previousMetadata != null
                && Objects.equals(
                    previousMetadata.customs.get(ComponentTemplateMetadata.TYPE),
                    this.customs.get(ComponentTemplateMetadata.TYPE)
                )) {
                systemTemplatesLookup = Collections.unmodifiableMap(previousMetadata.systemTemplatesLookup);
            } else {
                systemTemplatesLookup = new HashMap<>();
                Optional.ofNullable((ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE))
                    .map(ComponentTemplateMetadata::componentTemplates)
                    .orElseGet(Collections::emptyMap)
                    .forEach((k, v) -> {
                        if (v.metadata() != null && "system".equals(v.metadata().get("_type"))) {
                            SystemTemplateMetadata templateMetadata = SystemTemplateMetadata.fromComponentTemplate(k);
                            systemTemplatesLookup.compute(templateMetadata.name(), (ik, iv) -> {
                                if (iv == null) {
                                    iv = new TreeMap<>();
                                }
                                iv.put(templateMetadata.version(), k);
                                return iv;
                            });
                        }
                    });
            }
        }

        protected Metadata buildMetadataWithPreviousIndicesLookups() {
            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                hashesOfConsistentSettings,
                indices,
                templates,
                customs,
                Arrays.copyOf(previousMetadata.allIndices, previousMetadata.allIndices.length),
                Arrays.copyOf(previousMetadata.visibleIndices, previousMetadata.visibleIndices.length),
                Arrays.copyOf(previousMetadata.allOpenIndices, previousMetadata.allOpenIndices.length),
                Arrays.copyOf(previousMetadata.visibleOpenIndices, previousMetadata.visibleOpenIndices.length),
                Arrays.copyOf(previousMetadata.allClosedIndices, previousMetadata.allClosedIndices.length),
                Arrays.copyOf(previousMetadata.visibleClosedIndices, previousMetadata.visibleClosedIndices.length),
                Collections.unmodifiableSortedMap(previousMetadata.indicesLookup),
                systemTemplatesLookup
            );
        }

        protected Metadata buildMetadataWithRecomputedIndicesLookups() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will be rebuilt only when needed. Now during serializing we rebuild these datastructures
            // while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final List<String> visibleIndices = new ArrayList<>();
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> visibleOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final List<String> visibleClosedIndices = new ArrayList<>();
            final Set<String> allAliases = new HashSet<>();
            for (final IndexMetadata indexMetadata : indices.values()) {
                final String name = indexMetadata.getIndex().getName();
                boolean added = allIndices.add(name);
                assert added : "double index named [" + name + "]";
                final boolean visible = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings()) == false;
                if (visible) {
                    visibleIndices.add(name);
                }
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                    allOpenIndices.add(name);
                    if (visible) {
                        visibleOpenIndices.add(name);
                    }
                } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    allClosedIndices.add(name);
                    if (visible) {
                        visibleClosedIndices.add(name);
                    }
                }
                indexMetadata.getAliases().keySet().iterator().forEachRemaining(allAliases::add);
            }

            final Set<String> allDataStreams = new HashSet<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            if (dataStreamMetadata != null) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    allDataStreams.add(dataStream.getName());
                }
            }

            final Set<String> aliasDuplicatesWithIndices = new HashSet<>(allAliases);
            aliasDuplicatesWithIndices.retainAll(allIndices);
            ArrayList<String> duplicates = new ArrayList<>();
            if (aliasDuplicatesWithIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (final IndexMetadata cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithIndices) {
                        if (cursor.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with index");
                        }
                    }
                }
            }

            final Set<String> aliasDuplicatesWithDataStreams = new HashSet<>(allAliases);
            aliasDuplicatesWithDataStreams.retainAll(allDataStreams);
            if (aliasDuplicatesWithDataStreams.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (final IndexMetadata cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithDataStreams) {
                        if (cursor.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with data stream");
                        }
                    }
                }
            }

            final Set<String> dataStreamDuplicatesWithIndices = new HashSet<>(allDataStreams);
            dataStreamDuplicatesWithIndices.retainAll(allIndices);
            if (dataStreamDuplicatesWithIndices.isEmpty() == false) {
                for (String dataStream : dataStreamDuplicatesWithIndices) {
                    duplicates.add("data stream [" + dataStream + "] conflicts with index");
                }
            }

            if (duplicates.size() > 0) {
                throw new IllegalStateException(
                    "index, alias, and data stream names need to be unique, but the following duplicates "
                        + "were found ["
                        + Strings.collectionToCommaDelimitedString(duplicates)
                        + "]"
                );
            }

            SortedMap<String, IndexAbstraction> indicesLookup = Collections.unmodifiableSortedMap(buildIndicesLookup());

            validateDataStreams(indicesLookup, (DataStreamMetadata) customs.get(DataStreamMetadata.TYPE));

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] allIndicesArray = allIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleIndicesArray = visibleIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allOpenIndicesArray = allOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allClosedIndicesArray = allClosedIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(Strings.EMPTY_ARRAY);

            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                hashesOfConsistentSettings,
                indices,
                templates,
                customs,
                allIndicesArray,
                visibleIndicesArray,
                allOpenIndicesArray,
                visibleOpenIndicesArray,
                allClosedIndicesArray,
                visibleClosedIndicesArray,
                indicesLookup,
                systemTemplatesLookup
            );
        }

        private SortedMap<String, IndexAbstraction> buildIndicesLookup() {
            SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
            Map<String, DataStream> indexToDataStreamLookup = new HashMap<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            // If there are no indices, then skip data streams. This happens only when metadata is read from disk
            if (dataStreamMetadata != null && indices.size() > 0) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    List<IndexMetadata> backingIndices = dataStream.getIndices()
                        .stream()
                        .map(index -> indices.get(index.getName()))
                        .collect(Collectors.toList());
                    assert backingIndices.isEmpty() == false;
                    assert backingIndices.contains(null) == false;

                    IndexAbstraction existing = indicesLookup.put(
                        dataStream.getName(),
                        new IndexAbstraction.DataStream(dataStream, backingIndices)
                    );
                    assert existing == null : "duplicate data stream for " + dataStream.getName();

                    for (Index i : dataStream.getIndices()) {
                        indexToDataStreamLookup.put(i.getName(), dataStream);
                    }
                }
            }

            for (final IndexMetadata indexMetadata : indices.values()) {
                IndexAbstraction.Index index;
                DataStream parent = indexToDataStreamLookup.get(indexMetadata.getIndex().getName());
                if (parent != null) {
                    assert parent.getIndices().contains(indexMetadata.getIndex());
                    index = new IndexAbstraction.Index(indexMetadata, (IndexAbstraction.DataStream) indicesLookup.get(parent.getName()));
                } else {
                    index = new IndexAbstraction.Index(indexMetadata);
                }

                IndexAbstraction existing = indicesLookup.put(indexMetadata.getIndex().getName(), index);
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (final AliasMetadata aliasMetadata : indexMetadata.getAliases().values()) {
                    indicesLookup.compute(aliasMetadata.getAlias(), (aliasName, alias) -> {
                        if (alias == null) {
                            return new IndexAbstraction.Alias(aliasMetadata, indexMetadata);
                        } else {
                            assert alias.getType() == IndexAbstraction.Type.ALIAS : alias.getClass().getName();
                            ((IndexAbstraction.Alias) alias).addIndex(indexMetadata);
                            return alias;
                        }
                    });
                }
            }

            indicesLookup.values()
                .stream()
                .filter(aliasOrIndex -> aliasOrIndex.getType() == IndexAbstraction.Type.ALIAS)
                .forEach(alias -> ((IndexAbstraction.Alias) alias).computeAndValidateAliasProperties());
            return indicesLookup;
        }

        /**
         * Validates there isn't any index with a name that would clash with the future backing indices of the existing data streams.
         * <p>
         * E.g., if data stream `foo` has backing indices [`.ds-foo-000001`, `.ds-foo-000002`] and the indices lookup contains indices
         * `.ds-foo-000001`, `.ds-foo-000002` and `.ds-foo-000006` this will throw an IllegalStateException (as attempting to rollover the
         * `foo` data stream from generation 5 to 6 will not be possible)
         *
         * @param indicesLookup the indices in the system (this includes the data stream backing indices)
         * @param dsMetadata    the data streams in the system
         */
        static void validateDataStreams(SortedMap<String, IndexAbstraction> indicesLookup, @Nullable DataStreamMetadata dsMetadata) {
            if (dsMetadata != null) {
                for (DataStream ds : dsMetadata.dataStreams().values()) {
                    String prefix = DataStream.BACKING_INDEX_PREFIX + ds.getName() + "-";
                    Set<String> conflicts = indicesLookup.subMap(prefix, DataStream.BACKING_INDEX_PREFIX + ds.getName() + ".")
                        .keySet()
                        .stream()
                        .filter(s -> NUMBER_PATTERN.matcher(s.substring(prefix.length())).matches())
                        .filter(s -> IndexMetadata.parseIndexNameCounter(s) > ds.getGeneration())
                        .collect(Collectors.toSet());

                    if (conflicts.size() > 0) {
                        throw new IllegalStateException(
                            "data stream ["
                                + ds.getName()
                                + "] could create backing indices that conflict with "
                                + conflicts.size()
                                + " existing index(s) or alias(s)"
                                + " including '"
                                + conflicts.iterator().next()
                                + "'"
                        );
                    }
                }
            }
        }

        public static void toXContent(Metadata metadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));

            if (context == XContentContext.API) {
                builder.startObject("metadata");
            } else {
                builder.startObject("meta-data");
                builder.field("version", metadata.version());
            }

            builder.field("cluster_uuid", metadata.clusterUUID);
            builder.field("cluster_uuid_committed", metadata.clusterUUIDCommitted);

            builder.startObject("cluster_coordination");
            metadata.coordinationMetadata().toXContent(builder, params);
            builder.endObject();

            if (context != XContentContext.API && !metadata.persistentSettings().isEmpty()) {
                builder.startObject("settings");
                metadata.persistentSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            builder.startObject("templates");
            metadata.templatesMetadata().toXContent(builder, params);
            builder.endObject();

            if (context == XContentContext.API) {
                builder.startObject("indices");
                for (IndexMetadata indexMetadata : metadata) {
                    IndexMetadata.Builder.toXContent(indexMetadata, builder, params);
                }
                builder.endObject();
            }

            for (final Map.Entry<String, Custom> cursor : metadata.customs().entrySet()) {
                if (cursor.getValue().context().contains(context)) {
                    builder.startObject(cursor.getKey());
                    cursor.getValue().toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

}
