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

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.OpenSearchGenerationException;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.codelibs.fesen.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.codelibs.fesen.opensearch.common.settings.Settings.writeSettingsToStream;
import static org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.codelibs.fesen.opensearch.core.common.Strings.EMPTY_ARRAY;

/**
 * Create snapshot request
 * <p>
 * The only mandatory parameter is repository name. The repository name has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('_')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file name characters {@link Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateSnapshotRequest extends ClusterManagerNodeRequest<CreateSnapshotRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {
    public static int MAXIMUM_METADATA_BYTES = 1024; // chosen arbitrarily

    private String snapshot;

    private String repository;

    private String[] indices = EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenHidden();

    private boolean partial = false;

    private Settings settings = EMPTY_SETTINGS;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    private Map<String, Object> userMetadata;

    public CreateSnapshotRequest() {}

    /**
     * Constructs a new put repository request with the provided snapshot and repository names
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public CreateSnapshotRequest(String repository, String snapshot) {
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
        writeSettingsToStream(settings, out);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
        out.writeMap(userMetadata);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("snapshot is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }
        if (settings == null) {
            validationException = addValidationError("settings is null", validationException);
        }
        final int metadataSize = metadataSize(userMetadata);
        if (metadataSize > MAXIMUM_METADATA_BYTES) {
            validationException = addValidationError(
                "metadata must be smaller than 1024 bytes, but was [" + metadataSize + "]",
                validationException
            );
        }
        return validationException;
    }

    public static int metadataSize(Map<String, Object> userMetadata) {
        if (userMetadata == null) {
            return 0;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.value(userMetadata);
            int size = BytesReference.bytes(builder).length();
            return size;
        } catch (IOException e) {
            // This should not be possible as we are just rendering the xcontent in memory
            throw new OpenSearchException(e);
        }
    }

    /**
     * The snapshot name
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
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this request
     */
    @Override
    public CreateSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Returns a list of indices that should be included into the snapshot
     *
     * @return array of index names
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices options
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * If set to true the operation should wait for the snapshot completion before returning.
     * <p>
     * By default, the operation will return as soon as snapshot is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot completion
     * @return this request
     */
    public CreateSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("snapshot", snapshot);
        builder.startArray("indices");
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.field("partial", partial);
        if (settings != null) {
            builder.startObject("settings");
            if (settings.isEmpty() == false) {
                settings.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.field("include_global_state", includeGlobalState);
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        builder.field("metadata", userMetadata);
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
        CreateSnapshotRequest that = (CreateSnapshotRequest) o;
        return partial == that.partial
            && includeGlobalState == that.includeGlobalState
            && waitForCompletion == that.waitForCompletion
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(settings, that.settings)
            && Objects.equals(clusterManagerNodeTimeout, that.clusterManagerNodeTimeout)
            && Objects.equals(userMetadata, that.userMetadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            snapshot,
            repository,
            indicesOptions,
            partial,
            settings,
            includeGlobalState,
            waitForCompletion,
            userMetadata
        );
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return "CreateSnapshotRequest{"
            + "snapshot='"
            + snapshot
            + '\''
            + ", repository='"
            + repository
            + '\''
            + ", indices="
            + (indices == null ? null : Arrays.asList(indices))
            + ", indicesOptions="
            + indicesOptions
            + ", partial="
            + partial
            + ", settings="
            + settings
            + ", includeGlobalState="
            + includeGlobalState
            + ", waitForCompletion="
            + waitForCompletion
            + ", clusterManagerNodeTimeout="
            + clusterManagerNodeTimeout
            + ", metadata="
            + userMetadata
            + '}';
    }
}
