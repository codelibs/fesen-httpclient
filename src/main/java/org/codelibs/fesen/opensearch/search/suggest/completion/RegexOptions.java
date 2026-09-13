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

package org.codelibs.fesen.opensearch.search.suggest.completion;

import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.query.RegexpFlag;

import java.io.IOException;

/**
 * Regular expression options for completion suggester
 *
 * @opensearch.internal
 */
public class RegexOptions implements ToXContentFragment, Writeable {
    static final ParseField REGEX_OPTIONS = new ParseField("regex");
    private static final ParseField FLAGS_VALUE = new ParseField("flags", "flags_value");
    private static final ParseField MAX_DETERMINIZED_STATES = new ParseField("max_determinized_states");

    /**
     * regex: {
     *     "flags" : STRING | INT
     *     "max_determinized_states" : INT
     * }
     */
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(REGEX_OPTIONS.getPreferredName(), Builder::new);
    static {
        PARSER.declareInt(Builder::setMaxDeterminizedStates, MAX_DETERMINIZED_STATES);
        PARSER.declareField((parser, builder, aVoid) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                builder.setFlags(parser.text());
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                builder.setFlagsValue(parser.intValue());
            } else {
                throw new OpenSearchParseException(
                    REGEX_OPTIONS.getPreferredName() + " " + FLAGS_VALUE.getPreferredName() + " supports string or number"
                );
            }
        }, FLAGS_VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareStringOrNull(Builder::setFlags, FLAGS_VALUE);
    }

    static RegexOptions parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    private int flagsValue;
    private int maxDeterminizedStates;

    private RegexOptions(int flagsValue, int maxDeterminizedStates) {
        this.flagsValue = flagsValue;
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    /**
     * Read from a stream.
     */
    RegexOptions(StreamInput in) throws IOException {
        this.flagsValue = in.readVInt();
        this.maxDeterminizedStates = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(flagsValue);
        out.writeVInt(maxDeterminizedStates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegexOptions that = (RegexOptions) o;

        if (flagsValue != that.flagsValue) return false;
        return maxDeterminizedStates == that.maxDeterminizedStates;

    }

    @Override
    public int hashCode() {
        int result = flagsValue;
        result = 31 * result + maxDeterminizedStates;
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(REGEX_OPTIONS.getPreferredName());
        builder.field(FLAGS_VALUE.getPreferredName(), flagsValue);
        builder.field(MAX_DETERMINIZED_STATES.getPreferredName(), maxDeterminizedStates);
        builder.endObject();
        return builder;
    }

    /**
     * Options for regular expression queries
     *
     * @opensearch.internal
     */
    public static class Builder {
        private int flagsValue = RegExp.ALL;
        private int maxDeterminizedStates = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

        public Builder() {}

        /**
         * Sets the regular expression syntax flags
         * see {@link RegexpFlag}
         */
        public Builder setFlags(String flags) {
            this.flagsValue = RegexpFlag.resolveValue(flags);
            return this;
        }

        private Builder setFlagsValue(int flagsValue) {
            this.flagsValue = flagsValue;
            return this;
        }

        /**
         * Sets the maximum automaton states allowed for the regular expression expansion
         */
        public Builder setMaxDeterminizedStates(int maxDeterminizedStates) {
            if (maxDeterminizedStates < 0) {
                throw new IllegalArgumentException("maxDeterminizedStates must not be negative");
            }
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        public RegexOptions build() {
            return new RegexOptions(flagsValue, maxDeterminizedStates);
        }
    }
}
