/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.mapper;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.script.Script;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * DerivedField representation: expects a name, type and script.
 */
@PublicApi(since = "2.14.0")
public class DerivedField implements Writeable, ToXContentFragment {
    private final String name;
    private final String type;
    private final Script script;
    private String prefilterField;
    private Map<String, Object> properties;
    private Boolean ignoreMalformed;
    private String format;

    /**
     * Creates a new DerivedField.
     *
     * @param name the name
     * @param type the type
     * @param script the script
     */
    public DerivedField(String name, String type, Script script) {
        this.name = name;
        this.type = type;
        this.script = script;
    }

    /**
     * Creates a new DerivedField by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public DerivedField(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        script = new Script(in);
        if (in.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (in.readBoolean()) {
                properties = in.readMap();
            }
            prefilterField = in.readOptionalString();
            format = in.readOptionalString();
            ignoreMalformed = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        script.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (properties == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(properties);
            }
            out.writeOptionalString(prefilterField);
            out.writeOptionalString(format);
            out.writeOptionalBoolean(ignoreMalformed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", type);
        builder.field("script", script);
        if (properties != null) {
            builder.field("properties", properties);
        }
        if (prefilterField != null) {
            builder.field("prefilter_field", prefilterField);
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (ignoreMalformed != null) {
            builder.field("ignore_malformed", ignoreMalformed);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, script, prefilterField, properties, ignoreMalformed, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DerivedField other = (DerivedField) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(type, other.type)
            && Objects.equals(script, other.script)
            && Objects.equals(prefilterField, other.prefilterField)
            && Objects.equals(properties, other.properties)
            && Objects.equals(ignoreMalformed, other.ignoreMalformed)
            && Objects.equals(format, other.format);
    }
}
