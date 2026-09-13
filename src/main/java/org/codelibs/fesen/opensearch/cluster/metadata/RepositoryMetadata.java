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

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata about registered repository
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RepositoryMetadata implements Writeable {

    /**
     * The generation of a repository whose root blob has not been read yet.
     */
    public static final long UNKNOWN_REPO_GEN = -2L;

    /**
     * The generation of an empty repository.
     */
    public static final long EMPTY_REPO_GEN = -1L;

    private final String name;
    private final String type;
    private final Settings settings;
    private final CryptoMetadata cryptoMetadata;

    /**
     * Safe repository generation.
     */
    private final long generation;

    /**
     * Pending repository generation.
     */
    private final long pendingGeneration;

    public RepositoryMetadata(String name, String type, Settings settings, CryptoMetadata cryptoMetadata) {
        this(name, type, settings, UNKNOWN_REPO_GEN, EMPTY_REPO_GEN, cryptoMetadata);
    }

    public RepositoryMetadata(
        String name,
        String type,
        Settings settings,
        long generation,
        long pendingGeneration,
        CryptoMetadata cryptoMetadata
    ) {
        this.name = name;
        this.type = type;
        this.settings = settings;
        this.generation = generation;
        this.pendingGeneration = pendingGeneration;
        assert generation <= pendingGeneration : "Pending generation ["
            + pendingGeneration
            + "] must be greater or equal to generation ["
            + generation
            + "]";
        this.cryptoMetadata = cryptoMetadata;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String name() {
        return this.name;
    }

    /**
     * Returns repository type
     *
     * @return repository type
     */
    public String type() {
        return this.type;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Returns crypto metadata of repository
     *
     * @return crypto metadata of repository
     */
    public CryptoMetadata cryptoMetadata() {
        return this.cryptoMetadata;
    }

    /**
     * Returns the safe repository generation. the repository data for this generation is assumed to exist in the repository.
     * All operations on the repository must be based on the the repository data at this generation.
     * See package level documentation for the blob store based repositories {@link org.codelibs.fesen.opensearch.repositories.blobstore} for details
     * on how this value is used during snapshots.
     * @return safe repository generation
     */
    public long generation() {
        return generation;
    }

    /**
     * Returns the pending repository generation. the repository data for this generation and all generations down to the safe
     * generation {@link #generation} may exist in the repository and should not be reused for writing new the repository data to the
     * repository.
     * See package level documentation for the blob store based repositories {@link org.codelibs.fesen.opensearch.repositories.blobstore} for details
     * on how this value is used during snapshots.
     *
     * @return highest pending repository generation
     */
    public long pendingGeneration() {
        return pendingGeneration;
    }

    public RepositoryMetadata(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        settings = Settings.readSettingsFromStream(in);
        generation = in.readLong();
        pendingGeneration = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            cryptoMetadata = in.readOptionalWriteable(CryptoMetadata::new);
        } else {
            cryptoMetadata = null;
        }
    }

    /**
     * Writes repository metadata to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        Settings.writeSettingsToStream(settings, out);
        out.writeLong(generation);
        out.writeLong(pendingGeneration);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalWriteable(cryptoMetadata);
        }
    }

    /**
     * Checks if this instance is equal to the other instance in all fields other than {@link #generation} and {@link #pendingGeneration}.
     *
     * @param other other repository metadata
     * @return {@code true} if both instances equal in all fields but the generation fields
     */
    public boolean equalsIgnoreGenerations(RepositoryMetadata other) {
        return name.equals(other.name)
            && type.equals(other.type())
            && settings.equals(other.settings())
            && Objects.equals(cryptoMetadata, other.cryptoMetadata());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryMetadata that = (RepositoryMetadata) o;

        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        if (generation != that.generation) return false;
        if (pendingGeneration != that.pendingGeneration) return false;
        if (!settings.equals(that.settings)) return false;
        return Objects.equals(cryptoMetadata, that.cryptoMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, settings, generation, pendingGeneration, cryptoMetadata);
    }

    @Override
    public String toString() {
        String toStr = "RepositoryMetadata{" + name + "}{" + type + "}{" + settings + "}{" + generation + "}{" + pendingGeneration + "}";
        if (cryptoMetadata != null) {
            return toStr + "{" + cryptoMetadata + "}";
        }
        return toStr;
    }
}
