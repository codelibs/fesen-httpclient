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

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.cluster.AbstractNamedDiffable;
import org.codelibs.fesen.opensearch.cluster.NamedDiff;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata.Custom;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Contains metadata about registered snapshot repositories
 *
 * @opensearch.internal
 */
public class RepositoriesMetadata extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "repositories";

    /**
     * Serialization parameter used to hide the {@link RepositoryMetadata#generation()} and {@link RepositoryMetadata#pendingGeneration()}
     * in {@link org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse}.
     */
    public static final String HIDE_GENERATIONS_PARAM = "hide_generations";
    public static final String HIDE_SYSTEM_REPOSITORY_SETTING = "hide_system_repository_setting";

    /**
     * Marks a repository the system registered for itself rather than one a user created.
     */
    public static final Setting<Boolean> SYSTEM_REPOSITORY_SETTING = Setting.boolSetting(
        "system_repository",
        false,
        Setting.Property.NodeScope
    );

    private final List<RepositoryMetadata> repositories;

    /**
     * Constructs new repository metadata
     *
     * @param repositories list of repositories
     */
    public RepositoriesMetadata(List<RepositoryMetadata> repositories) {
        this.repositories = Collections.unmodifiableList(repositories);
    }

    /**
     * Returns list of currently registered repositories
     *
     * @return list of repositories
     */
    public List<RepositoryMetadata> repositories() {
        return this.repositories;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoriesMetadata that = (RepositoriesMetadata) o;

        return repositories.equals(that.repositories);
    }

    /**
     * Checks if this instance and the given instance share the same repositories by checking that this instances' repositories and the
     * repositories in {@code other} are equal or only differ in their values of {@link RepositoryMetadata#generation()} and
     * {@link RepositoryMetadata#pendingGeneration()}.
     *
     * @param other other repositories metadata
     * @return {@code true} iff both instances contain the same repositories apart from differences in generations
     */
    public boolean equalsIgnoreGenerations(@Nullable RepositoriesMetadata other) {
        if (other == null) {
            return false;
        }
        if (other.repositories.size() != repositories.size()) {
            return false;
        }
        for (int i = 0; i < repositories.size(); i++) {
            if (repositories.get(i).equalsIgnoreGenerations(other.repositories.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return repositories.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public RepositoriesMetadata(StreamInput in) throws IOException {
        this.repositories = in.readList(RepositoryMetadata::new);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(repositories);
    }

    public static RepositoriesMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<RepositoryMetadata> repository = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                // move to next token if parsing the whole object
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse repository [{}], expected object", name);
                }
                String type = null;
                Settings settings = Settings.EMPTY;
                long generation = RepositoryMetadata.UNKNOWN_REPO_GEN;
                long pendingGeneration = RepositoryMetadata.EMPTY_REPO_GEN;
                CryptoMetadata cryptoMetadata = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("type".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            type = parser.text();
                        } else if ("settings".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new OpenSearchParseException("failed to parse repository [{}], incompatible params", name);
                            }
                            settings = Settings.fromXContent(parser);
                        } else if ("generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            generation = parser.longValue();
                        } else if ("pending_generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            pendingGeneration = parser.longValue();
                        } else if (CryptoMetadata.CRYPTO_METADATA_KEY.equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            cryptoMetadata = CryptoMetadata.fromXContent(parser);
                        } else {
                            throw new OpenSearchParseException(
                                "failed to parse repository [{}], unknown field [{}]",
                                name,
                                currentFieldName
                            );
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse repository [{}]", name);
                    }
                }
                if (type == null) {
                    throw new OpenSearchParseException("failed to parse repository [{}], missing repository type", name);
                }
                repository.add(new RepositoryMetadata(name, type, settings, generation, pendingGeneration, cryptoMetadata));
            } else {
                throw new OpenSearchParseException("failed to parse repositories");
            }
        }
        return new RepositoriesMetadata(repository);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        for (RepositoryMetadata repository : repositories) {
            toXContent(repository, builder, params);
        }
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    /**
     * Serializes information about a single repository
     *
     * @param repository repository metadata
     * @param builder    XContent builder
     * @param params     serialization parameters
     */
    public static void toXContent(RepositoryMetadata repository, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(repository.name());
        builder.field("type", repository.type());
        if (repository.cryptoMetadata() != null) {
            repository.cryptoMetadata().toXContent(repository.cryptoMetadata(), builder, params);
        }
        Settings settings = repository.settings();
        if (SYSTEM_REPOSITORY_SETTING.get(settings) && params.paramAsBoolean(HIDE_SYSTEM_REPOSITORY_SETTING, false)) {
            settings = repository.settings().filter(s -> !s.equals(SYSTEM_REPOSITORY_SETTING.getKey()));
        }
        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        if (params.paramAsBoolean(HIDE_GENERATIONS_PARAM, false) == false) {
            builder.field("generation", repository.generation());
            builder.field("pending_generation", repository.pendingGeneration());
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
