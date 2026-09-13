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

package org.codelibs.fesen.opensearch.action.admin.cluster.shards;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport request for searching shards
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterSearchShardsRequest extends ClusterManagerNodeReadRequest<ClusterSearchShardsRequest>
    implements
        IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    @Nullable
    private String routing;
    @Nullable
    private String preference;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();
    @Nullable
    private SliceBuilder sliceBuilder;

    public ClusterSearchShardsRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_2_19_0)) {
            if (sliceBuilder != null) {
                out.writeBoolean(true);
                sliceBuilder.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public ClusterSearchShardsRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (int i = 0; i < indices.length; i++) {
            Objects.requireNonNull(indices[i], "indices[" + i + "] must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * The indices
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
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    public ClusterSearchShardsRequest slice(SliceBuilder sliceBuilder) {
        this.sliceBuilder = sliceBuilder;
        return this;
    }

    public SliceBuilder slice() {
        return this.sliceBuilder;
    }
}
