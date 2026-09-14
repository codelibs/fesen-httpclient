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

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.codelibs.fesen.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.codelibs.fesen.opensearch.common.settings.Settings.writeSettingsToStream;
import static org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Restore snapshot request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RestoreSnapshotRequest extends ClusterManagerNodeRequest<RestoreSnapshotRequest> implements ToXContentObject {

    /** The longest an index name may be, in bytes. */
    private static final int MAX_INDEX_NAME_BYTES = 255;

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestoreSnapshotRequest.class);

    /**
     * Enumeration of possible storage types
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum StorageType {
        /**
         * The LOCAL value.
         */
        LOCAL("local"),
        /**
         * The REMOTE_SNAPSHOT value.
         */
        REMOTE_SNAPSHOT("remote_snapshot");

        private final String text;

        StorageType(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }

        private void toXContent(XContentBuilder builder) throws IOException {
            builder.field("storage_type", text);
        }
    }

    private String snapshot;
    private String repository;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String renamePattern;
    private String renameReplacement;
    private String renameAliasPattern;
    private String renameAliasReplacement;
    private boolean waitForCompletion;
    private boolean includeGlobalState = false;
    private boolean partial = false;
    private boolean includeAliases = true;
    private Settings indexSettings = EMPTY_SETTINGS;
    private String[] ignoreIndexSettings = Strings.EMPTY_ARRAY;
    private StorageType storageType = StorageType.LOCAL;
    @Nullable
    private String sourceRemoteStoreRepository = null;
    @Nullable
    private String sourceRemoteTranslogRepository = null;

    @Nullable // if any snapshot UUID will do
    private String snapshotUuid;

    /**
     * Alias write index policy for controlling how writeIndex attribute is handled during restore
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.3.0")
    public enum AliasWriteIndexPolicy {
        /**
         * The PRESERVE value.
         */
        PRESERVE,
        /**
         * The STRIP_WRITE_INDEX value.
         */
        STRIP_WRITE_INDEX;
    }

    private AliasWriteIndexPolicy aliasWriteIndexPolicy = AliasWriteIndexPolicy.PRESERVE;

    private boolean attachToDataStream = false;

    /**
     * Creates a new RestoreSnapshotRequest.
     */
    public RestoreSnapshotRequest() {}

    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public RestoreSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(renamePattern);
        out.writeOptionalString(renameReplacement);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(partial);
        out.writeBoolean(includeAliases);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoreIndexSettings);
        out.writeOptionalString(snapshotUuid);
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeEnum(storageType);
        }
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalString(sourceRemoteStoreRepository);
        }
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeOptionalString(sourceRemoteTranslogRepository);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalString(renameAliasPattern);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalString(renameAliasReplacement);
        }
        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            out.writeEnum(aliasWriteIndexPolicy);
        }
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeBoolean(attachToDataStream);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices are missing", validationException);
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is missing", validationException);
        }
        if (indexSettings == null) {
            validationException = addValidationError("indexSettings are missing", validationException);
        }
        if (ignoreIndexSettings == null) {
            validationException = addValidationError("ignoreIndexSettings are missing", validationException);
        }
        if (Strings.isNullOrEmpty(renameReplacement) == false
            && renameReplacement.getBytes(StandardCharsets.UTF_8).length > MAX_INDEX_NAME_BYTES) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "rename_replacement string size exceeds max allowed size of %s bytes",
                    MAX_INDEX_NAME_BYTES
                ),
                validationException
            );
        }
        return validationException;
    }

    /**
     * Returns the name of the snapshot.
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public RestoreSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("indices");
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        if (renamePattern != null) {
            builder.field("rename_pattern", renamePattern);
        }
        if (renameReplacement != null) {
            builder.field("rename_replacement", renameReplacement);
        }
        if (renameAliasPattern != null) {
            builder.field("rename_alias_pattern", renameAliasPattern);
        }
        if (renameAliasReplacement != null) {
            builder.field("rename_alias_replacement", renameAliasReplacement);
        }
        builder.field("include_global_state", includeGlobalState);
        builder.field("partial", partial);
        builder.field("include_aliases", includeAliases);
        if (indexSettings != null) {
            builder.startObject("index_settings");
            if (indexSettings.isEmpty() == false) {
                indexSettings.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.startArray("ignore_index_settings");
        for (String ignoreIndexSetting : ignoreIndexSettings) {
            builder.value(ignoreIndexSetting);
        }
        builder.endArray();
        if (storageType != null) {
            storageType.toXContent(builder);
        }
        if (sourceRemoteStoreRepository != null) {
            builder.field("source_remote_store_repository", sourceRemoteStoreRepository);
        }
        if (sourceRemoteTranslogRepository != null) {
            builder.field("source_remote_translog_repository", sourceRemoteTranslogRepository);
        }
        builder.field("alias_write_index_policy", aliasWriteIndexPolicy.name().toLowerCase(Locale.ROOT));
        builder.field("attach_to_data_stream", attachToDataStream);
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreSnapshotRequest that = (RestoreSnapshotRequest) o;
        boolean equals = waitForCompletion == that.waitForCompletion
            && includeGlobalState == that.includeGlobalState
            && partial == that.partial
            && includeAliases == that.includeAliases
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(renamePattern, that.renamePattern)
            && Objects.equals(renameReplacement, that.renameReplacement)
            && Objects.equals(renameAliasPattern, that.renameAliasPattern)
            && Objects.equals(renameAliasReplacement, that.renameAliasReplacement)
            && Objects.equals(indexSettings, that.indexSettings)
            && Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings)
            && Objects.equals(snapshotUuid, that.snapshotUuid)
            && Objects.equals(storageType, that.storageType)
            && Objects.equals(sourceRemoteStoreRepository, that.sourceRemoteStoreRepository)
            && Objects.equals(sourceRemoteTranslogRepository, that.sourceRemoteTranslogRepository)
            && aliasWriteIndexPolicy == that.aliasWriteIndexPolicy
            && attachToDataStream == that.attachToDataStream;
        return equals;
    }

    @Override
    public int hashCode() {
        int result;
        result = Objects.hash(
            snapshot,
            repository,
            indicesOptions,
            renamePattern,
            renameReplacement,
            renameAliasPattern,
            renameAliasReplacement,
            waitForCompletion,
            includeGlobalState,
            partial,
            includeAliases,
            indexSettings,
            snapshotUuid,
            storageType,
            sourceRemoteStoreRepository,
            sourceRemoteTranslogRepository,
            aliasWriteIndexPolicy,
            attachToDataStream
        );
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
