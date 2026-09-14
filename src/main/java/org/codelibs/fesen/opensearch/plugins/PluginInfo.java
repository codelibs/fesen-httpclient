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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.plugins;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContentParser;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.semver.SemverRange;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonFactoryBuilder;
import tools.jackson.core.json.JsonReadFeature;

import static org.codelibs.fesen.opensearch.semver.SemverRange.RANGE_PATTERN;

/**
 * An in-memory representation of the plugin descriptor.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PluginInfo implements Writeable, ToXContentObject {

    private static final JsonFactory jsonFactory = new JsonFactoryBuilder().configure(JsonReadFeature.ALLOW_UNQUOTED_PROPERTY_NAMES, true)
        .build();

    private final String name;
    private final String description;
    private final String version;
    private final List<SemverRange> opensearchVersionRanges;
    private final String javaVersion;
    private final String classname;
    private final String customFolderName;
    private final List<String> extendedPlugins;
    // Optional extended plugins are a subset of extendedPlugins that only contains the optional extended plugins
    private final List<String> optionalExtendedPlugins;
    private final boolean hasNativeController;

    /**
     * Construct plugin info.
     *
     * @param name                  the name of the plugin
     * @param description           a description of the plugin
     * @param version               an opaque version identifier for the plugin
     * @param opensearchVersion     the version of OpenSearch the plugin was built for
     * @param javaVersion           the version of Java the plugin was built with
     * @param classname             the entry point to the plugin
     * @param customFolderName      the custom folder name for the plugin
     * @param extendedPlugins       other plugins this plugin extends through SPI
     * @param hasNativeController   whether or not the plugin has a native controller
     */
    public PluginInfo(
        String name,
        String description,
        String version,
        Version opensearchVersion,
        String javaVersion,
        String classname,
        String customFolderName,
        List<String> extendedPlugins,
        boolean hasNativeController
    ) {
        this(
            name,
            description,
            version,
            List.of(SemverRange.fromString(opensearchVersion.toString())),
            javaVersion,
            classname,
            customFolderName,
            extendedPlugins,
            hasNativeController
        );
    }

    /**
     * Creates a new PluginInfo.
     *
     * @param name the name
     * @param description the description
     * @param version the version
     * @param opensearchVersionRanges the opensearch version ranges
     * @param javaVersion the java version
     * @param classname the classname
     * @param customFolderName the custom folder name
     * @param extendedPlugins the extended plugins
     * @param hasNativeController the has native controller
     */
    public PluginInfo(
        String name,
        String description,
        String version,
        List<SemverRange> opensearchVersionRanges,
        String javaVersion,
        String classname,
        String customFolderName,
        List<String> extendedPlugins,
        boolean hasNativeController
    ) {
        this.name = name;
        this.description = description;
        this.version = version;
        // Ensure only one range is specified (for now)
        if (opensearchVersionRanges.size() != 1) {
            throw new IllegalArgumentException(
                "Exactly one range is allowed to be specified in dependencies for the plugin [" + name + "]"
            );
        }
        this.opensearchVersionRanges = opensearchVersionRanges;
        this.javaVersion = javaVersion;
        this.classname = classname;
        this.customFolderName = customFolderName;
        this.extendedPlugins = extendedPlugins.stream().map(s -> s.split(";")[0]).collect(Collectors.toUnmodifiableList());
        this.optionalExtendedPlugins = extendedPlugins.stream()
            .filter(PluginInfo::isOptionalExtension)
            .map(s -> s.split(";")[0])
            .collect(Collectors.toUnmodifiableList());
        this.hasNativeController = hasNativeController;
    }

    /**
     * Construct plugin info from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    @SuppressWarnings("unchecked")
    public PluginInfo(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            this.opensearchVersionRanges = (List<SemverRange>) in.readGenericValue();
        } else {
            this.opensearchVersionRanges = List.of(new SemverRange(in.readVersion(), SemverRange.RangeOperator.DEFAULT));
        }
        this.javaVersion = in.readString();
        this.classname = in.readString();
        this.customFolderName = in.readString();
        this.extendedPlugins = in.readStringList();
        this.hasNativeController = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_19_0)) {
            this.optionalExtendedPlugins = in.readStringList();
        } else {
            this.optionalExtendedPlugins = new ArrayList<>();
        }

    }

    static boolean isOptionalExtension(String extendedPlugin) {
        String[] dependency = extendedPlugin.split(";");
        return dependency.length > 1 && "optional=true".equals(dependency[1]);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            out.writeGenericValue(opensearchVersionRanges);
        } else {
            /*
            This works for currently supported range notations (=,~)
            As more notations get added, then a suitable version must be picked.
             */
            out.writeVersion(opensearchVersionRanges.get(0).getRangeVersion());
        }
        out.writeString(javaVersion);
        out.writeString(classname);
        if (customFolderName != null) {
            out.writeString(customFolderName);
        } else {
            out.writeString(name);
        }
        out.writeStringCollection(extendedPlugins);
        out.writeBoolean(hasNativeController);
        if (out.getVersion().onOrAfter(Version.V_2_19_0)) {
            out.writeStringCollection(optionalExtendedPlugins);
        }
    }

    /**
     * The name of the plugin.
     *
     * @return the plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * Pretty print the semver ranges and return the string.
     * @return semver ranges string
     */
    public String getOpenSearchVersionRangesString() {
        if (opensearchVersionRanges == null || opensearchVersionRanges.isEmpty()) {
            throw new IllegalStateException("Opensearch version ranges list cannot be empty");
        }
        if (opensearchVersionRanges.size() == 1) {
            return opensearchVersionRanges.get(0).toString();
        }
        return opensearchVersionRanges.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("name", name);
            builder.field("version", version);
            builder.field("opensearch_version", getOpenSearchVersionRangesString());
            builder.field("java_version", javaVersion);
            builder.field("description", description);
            builder.field("classname", classname);
            builder.field("custom_foldername", customFolderName);
            builder.field("extended_plugins", extendedPlugins);
            builder.field("has_native_controller", hasNativeController);
            builder.field("optional_extended_plugins", optionalExtendedPlugins);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        if (!name.equals(that.name)) return false;
        // TODO: since the plugins are unique by their directory name, this should only be a name check, version should not matter?
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Returns a string representation of this instance.
     *
     * @param prefix the prefix
     * @return a string representation of this instance
     */
    public String toString(String prefix) {
        final StringBuilder information = new StringBuilder().append(prefix)
            .append("- Plugin information:\n")
            .append(prefix)
            .append("Name: ")
            .append(name)
            .append("\n")
            .append(prefix)
            .append("Description: ")
            .append(description)
            .append("\n")
            .append(prefix)
            .append("Version: ")
            .append(version)
            .append("\n")
            .append(prefix)
            .append("OpenSearch Version: ")
            .append(getOpenSearchVersionRangesString())
            .append("\n")
            .append(prefix)
            .append("Java Version: ")
            .append(javaVersion)
            .append("\n")
            .append(prefix)
            .append("Native Controller: ")
            .append(hasNativeController)
            .append("\n")
            .append(prefix)
            .append("Extended Plugins: ")
            .append(extendedPlugins)
            .append("\n")
            .append(prefix)
            .append(" * Classname: ")
            .append(classname)
            .append("\n")
            .append(prefix)
            .append("Folder name: ")
            .append(customFolderName);
        return information.toString();
    }

}
