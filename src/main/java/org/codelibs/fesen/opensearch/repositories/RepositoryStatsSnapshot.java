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

package org.codelibs.fesen.opensearch.repositories;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats snapshot about a repository
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RepositoryStatsSnapshot implements Writeable, ToXContentObject {
    private final RepositoryInfo repositoryInfo;
    private final RepositoryStats repositoryStats;
    private final long clusterVersion;

    /**
     * Creates a new RepositoryStatsSnapshot by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public RepositoryStatsSnapshot(StreamInput in) throws IOException {
        this.repositoryInfo = new RepositoryInfo(in);
        this.repositoryStats = new RepositoryStats(in);
        this.clusterVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositoryInfo.writeTo(out);
        repositoryStats.writeTo(out);
        out.writeLong(clusterVersion);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositoryInfo.toXContent(builder, params);
        repositoryStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStatsSnapshot that = (RepositoryStatsSnapshot) o;
        return repositoryInfo.equals(that.repositoryInfo)
            && repositoryStats.equals(that.repositoryStats)
            && clusterVersion == that.clusterVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryInfo, repositoryStats, clusterVersion);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
