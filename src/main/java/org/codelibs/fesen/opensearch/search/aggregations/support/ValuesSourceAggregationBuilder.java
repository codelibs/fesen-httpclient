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

package org.codelibs.fesen.opensearch.search.aggregations.support;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.AbstractObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.query.WithFieldName;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationInitializationException;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories.Builder;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSource;

/**
 * Base class for all values source agg builders
 *
 * @param <AB> the aggregation builder type
 * @opensearch.internal
 */
public abstract class ValuesSourceAggregationBuilder<AB extends ValuesSourceAggregationBuilder<AB>> extends AbstractAggregationBuilder<AB>
    implements
        WithFieldName {

    /**
     * Performs the declare fields step.
     *
     * @param <T> the element type
     * @param objectParser the object parser
     * @param scriptable the scriptable
     * @param formattable the formattable
     * @param timezoneAware the timezone aware
     */
    public static <T> void declareFields(
        AbstractObjectParser<? extends ValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean scriptable,
        boolean formattable,
        boolean timezoneAware
    ) {
        declareFields(objectParser, scriptable, formattable, timezoneAware, true);

    }

    /**
     * Performs the declare fields step.
     *
     * @param <T> the element type
     * @param objectParser the object parser
     * @param scriptable the scriptable
     * @param formattable the formattable
     * @param timezoneAware the timezone aware
     * @param fieldRequired the field required
     */
    public static <T> void declareFields(
        AbstractObjectParser<? extends ValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean scriptable,
        boolean formattable,
        boolean timezoneAware,
        boolean fieldRequired
    ) {

        objectParser.declareField(
            ValuesSourceAggregationBuilder::field,
            XContentParser::text,
            ParseField.CommonFields.FIELD,
            ObjectParser.ValueType.STRING
        );

        objectParser.declareField(
            ValuesSourceAggregationBuilder::missing,
            XContentParser::objectText,
            ParseField.CommonFields.MISSING,
            ObjectParser.ValueType.VALUE
        );

        objectParser.declareField(ValuesSourceAggregationBuilder::userValueTypeHint, p -> {
            ValueType type = ValueType.lenientParse(p.text());
            if (type == null) {
                throw new IllegalArgumentException("Unknown value type [" + p.text() + "]");
            }
            return type;
        }, ValueType.VALUE_TYPE, ObjectParser.ValueType.STRING);

        if (formattable) {
            objectParser.declareField(
                ValuesSourceAggregationBuilder::format,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }

        if (scriptable) {
            objectParser.declareField(
                ValuesSourceAggregationBuilder::script,
                (parser, context) -> Script.parse(parser),
                Script.SCRIPT_PARSE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
            if (fieldRequired) {
                String[] fields = new String[] {
                    ParseField.CommonFields.FIELD.getPreferredName(),
                    Script.SCRIPT_PARSE_FIELD.getPreferredName() };
                objectParser.declareRequiredFieldSet(fields);
            }
        } else {
            if (fieldRequired) {
                objectParser.declareRequiredFieldSet(ParseField.CommonFields.FIELD.getPreferredName());
            }
        }

        if (timezoneAware) {
            objectParser.declareField(ValuesSourceAggregationBuilder::timeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }
    }

    /**
     * Base leaf only
     *
     * @param <VS> the vs type
     * @param <AB> the aggregation builder type
     * @opensearch.internal
     */
    public abstract static class LeafOnly<VS extends ValuesSource, AB extends ValuesSourceAggregationBuilder<AB>> extends
        ValuesSourceAggregationBuilder<AB> {

        /**
         * Creates a new LeafOnly.
         *
         * @param name the name
         */
        protected LeafOnly(String name) {
            super(name);
        }

        /**
         * Creates a new LeafOnly.
         *
         * @param clone the clone
         * @param factoriesBuilder the factories builder
         * @param metadata the metadata
         */
        protected LeafOnly(LeafOnly<VS, AB> clone, Builder factoriesBuilder, Map<String, Object> metadata) {
            super(clone, factoriesBuilder, metadata);
            if (factoriesBuilder.count() > 0) {
                throw new AggregationInitializationException(
                    "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
                );
            }
        }

        /**
         * Read an aggregation from a stream
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        protected LeafOnly(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public final AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException(
                "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
            );
        }

        @Override
        public final BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }
    }

    private String field = null;
    private Script script = null;
    private ValueType userValueTypeHint = null;
    private String format = null;
    private Object missing = null;
    private ZoneId timeZone = null;
    /**
     * Creates a new ValuesSourceAggregationBuilder.
     *
     * @param name the name
     */
    protected ValuesSourceAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Creates a new ValuesSourceAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected ValuesSourceAggregationBuilder(
        ValuesSourceAggregationBuilder<AB> clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.field = clone.field;
        this.userValueTypeHint = clone.userValueTypeHint;
        this.format = clone.format;
        this.missing = clone.missing;
        this.timeZone = clone.timeZone;
        this.script = clone.script;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    protected ValuesSourceAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (serializeTargetValueType(in.getVersion())) {
            ValueType valueType = in.readOptionalWriteable(ValueType::readFromStream);
            assert valueType == null;
        }
        read(in);
    }

    /**
     * Read from a stream.
     */
    private void read(StreamInput in) throws IOException {
        field = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        if (in.readBoolean()) {
            userValueTypeHint = ValueType.readFromStream(in);
        }
        format = in.readOptionalString();
        missing = in.readGenericValue();
        timeZone = in.readOptionalZoneId();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        if (serializeTargetValueType(out.getVersion())) {
            // TODO: deprecate this so we don't need to carry around a useless null in the wire format
            out.writeOptionalWriteable(null);
        }
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        boolean hasValueType = userValueTypeHint != null;
        out.writeBoolean(hasValueType);
        if (hasValueType) {
            userValueTypeHint.writeTo(out);
        }
        out.writeOptionalString(format);
        out.writeGenericValue(missing);
        out.writeOptionalZoneId(timeZone);
        innerWriteTo(out);
    }

    /**
     * Write subclass's state to the stream.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * DO NOT OVERRIDE THIS!
     * <p>
     * This method only exists for legacy support.  No new aggregations need this, nor should they override it.
     *
     * @param version For backwards compatibility, subclasses can change behavior based on the version
     * @return this instance
     */
    protected boolean serializeTargetValueType(Version version) {
        return false;
    }

    /**
     * Sets the field to use for this aggregation.
     *
     * @param field the field
     * @return the field
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     *
     * @return the field
     */
    public String field() {
        return field;
    }

    @Override
    public String fieldName() {
        return field();
    }

    /**
     * Sets the script to use for this aggregation.
     *
     * @param script the script
     * @return the script
     */
    @SuppressWarnings("unchecked")
    public AB script(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("[script] must not be null: [" + name + "]");
        }
        this.script = script;
        return (AB) this;
    }

    /**
     * Gets the script to use for this aggregation.
     *
     * @return the script
     */
    public Script script() {
        return script;
    }

    /**
     * This setter should only be used during parsing, to set the userValueTypeHint.  This is information the user provides in the json
     * query to indicate the output type of a script or the type of the 'missing' replacement value.
     * @param valueType - The parsed {@link ValueType} based on the string the user specified
     * @return - The modified builder instance, for chaining.
     */
    @SuppressWarnings("unchecked")
    public AB userValueTypeHint(ValueType valueType) {
        if (valueType == null) {
            // TODO: This is nonsense. We allow the value to be null (via constructor), but don't allow it to be set to null. This means
            // thing looking to copy settings need to check if userValueTypeHint is not null, and then
            // set it if and only if it is non-null.
            throw new IllegalArgumentException("[userValueTypeHint] must not be null: [" + name + "]");
        }
        this.userValueTypeHint = valueType;
        return (AB) this;
    }

    /**
     * Returns the user value type hint.
     *
     * @return the user value type hint
     */
    public ValueType userValueTypeHint() {
        return userValueTypeHint;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     *
     * @param format the format
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public AB format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     *
     * @return this instance
     */
    public String format() {
        return format;
    }

    /**
     * Sets the value to use when the aggregation finds a missing value in a
     * document
     *
     * @param missing the missing
     * @return the missing
     */
    @SuppressWarnings("unchecked")
    public AB missing(Object missing) {
        if (missing == null) {
            throw new IllegalArgumentException("[missing] must not be null: [" + name + "]");
        }
        this.missing = missing;
        return (AB) this;
    }

    /**
     * Gets the value to use when the aggregation finds a missing value in a
     * document
     *
     * @return the missing
     */
    public Object missing() {
        return missing;
    }

    /**
     * Sets the time zone to use for this aggregation
     *
     * @param timeZone the time zone
     * @return the time zone
     */
    @SuppressWarnings("unchecked")
    public AB timeZone(ZoneId timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("[timeZone] must not be null: [" + name + "]");
        }
        this.timeZone = timeZone;
        return (AB) this;
    }

    /**
     * Gets the time zone to use for this aggregation
     *
     * @return the time zone
     */
    public ZoneId timeZone() {
        return timeZone;
    }

    /**
     * Aggregations should use this method to define a {@link ValuesSourceType} of last resort.  This will only be used when the resolver
     * can't find a field and the user hasn't provided a value type hint.
     *
     * @return The CoreValuesSourceType we expect this script to yield.
     */
    protected abstract ValuesSourceType defaultValueSourceType();

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        if (missing != null) {
            builder.field("missing", missing);
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone.toString());
        }
        if (userValueTypeHint != null) {
            builder.field("value_type", userValueTypeHint.getPreferredName());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the XContent body.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @return the XContent body
     * @throws IOException if an I/O error occurs
     */
    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, format, missing, script, timeZone, userValueTypeHint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ValuesSourceAggregationBuilder<?> other = (ValuesSourceAggregationBuilder<?>) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(format, other.format)
            && Objects.equals(missing, other.missing)
            && Objects.equals(script, other.script)
            && Objects.equals(timeZone, other.timeZone)
            && Objects.equals(userValueTypeHint, other.userValueTypeHint);
    }
}
