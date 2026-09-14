/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.support;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.IncludeExclude;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A configuration that used by multi_terms aggregations.
 *
 * @opensearch.internal
 */
public class MultiTermsValuesSourceConfig extends BaseMultiValuesSourceFieldConfig {
    private final ValueType userValueTypeHint;
    private final String format;
    private final IncludeExclude includeExclude;

    private static final String NAME = "field_config";
    /**
     * The FILTER constant.
     */
    public static final ParseField FILTER = new ParseField("filter");

    /**
     * Parser supplier function
     *
     * @opensearch.internal
     */
    public interface ParserSupplier {
        /**
         * Applies this instance to the given input.
         *
         * @param scriptable the scriptable
         * @param timezoneAware the timezone aware
         * @param valueTypeHinted the value type hinted
         * @param formatted the formatted
         * @return this instance
         */
        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> apply(
            Boolean scriptable,
            Boolean timezoneAware,
            Boolean valueTypeHinted,
            Boolean formatted
        );
    }

    /**
     * The PARSER constant.
     */
    public static final MultiTermsValuesSourceConfig.ParserSupplier PARSER = (scriptable, timezoneAware, valueTypeHinted, formatted) -> {

        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> parser = new ObjectParser<>(
            MultiTermsValuesSourceConfig.NAME,
            MultiTermsValuesSourceConfig.Builder::new
        );

        BaseMultiValuesSourceFieldConfig.PARSER.apply(parser, scriptable, timezoneAware);

        if (valueTypeHinted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setUserValueTypeHint,
                p -> ValueType.lenientParse(p.text()),
                ValueType.VALUE_TYPE,
                ObjectParser.ValueType.STRING
            );
        }

        if (formatted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setFormat,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }

        parser.declareField(
            (b, v) -> b.setIncludeExclude(IncludeExclude.merge(b.getIncludeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );

        return parser;
    };

    /**
     * Creates a new MultiTermsValuesSourceConfig.
     *
     * @param fieldName the field name
     * @param missing the missing
     * @param script the script
     * @param timeZone the time zone
     * @param userValueTypeHint the user value type hint
     * @param format the format
     * @param includeExclude the include exclude
     */
    protected MultiTermsValuesSourceConfig(
        String fieldName,
        Object missing,
        Script script,
        ZoneId timeZone,
        ValueType userValueTypeHint,
        String format,
        IncludeExclude includeExclude
    ) {
        super(fieldName, missing, script, timeZone);
        this.userValueTypeHint = userValueTypeHint;
        this.format = format;
        this.includeExclude = includeExclude;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(userValueTypeHint);
        out.writeOptionalString(format);
        out.writeOptionalWriteable(includeExclude);
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (userValueTypeHint != null) {
            builder.field(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field(AggregationBuilder.CommonFields.FORMAT.getPreferredName(), format);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;

        MultiTermsValuesSourceConfig that = (MultiTermsValuesSourceConfig) o;
        return Objects.equals(userValueTypeHint, that.userValueTypeHint)
            && Objects.equals(format, that.format)
            && Objects.equals(includeExclude, that.includeExclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userValueTypeHint, format, includeExclude);
    }

    /**
     * Builder for the multi terms values source configuration
     *
     * @opensearch.internal
     */
    public static class Builder extends BaseMultiValuesSourceFieldConfig.Builder<MultiTermsValuesSourceConfig, Builder> {
        /**
         * Creates a new Builder.
         */
        public Builder() {
        }

        private ValueType userValueTypeHint = null;
        private String format;
        private IncludeExclude includeExclude = null;

        /**
         * Returns the include exclude.
         *
         * @return the include exclude
         */
        public IncludeExclude getIncludeExclude() {
            return includeExclude;
        }

        /**
         * Sets the include exclude.
         *
         * @param includeExclude the include exclude
         * @return this instance
         */
        public Builder setIncludeExclude(IncludeExclude includeExclude) {
            this.includeExclude = includeExclude;
            return this;
        }

        /**
         * Sets the user value type hint.
         *
         * @param userValueTypeHint the user value type hint
         * @return this instance
         */
        public Builder setUserValueTypeHint(ValueType userValueTypeHint) {
            this.userValueTypeHint = userValueTypeHint;
            return this;
        }

        /**
         * Sets the format.
         *
         * @param format the format
         * @return this instance
         */
        public Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        public MultiTermsValuesSourceConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be null.  "
                        + "Please specify one or the other."
                );
            }
            return new MultiTermsValuesSourceConfig(fieldName, missing, script, timeZone, userValueTypeHint, format, includeExclude);
        }
    }
}
