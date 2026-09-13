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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.range;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ContextParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser.ValueType;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Namespace for the range-aggregation request types shared by builders and responses.
 *
 * <p>The aggregator itself is node-side and is not carried over; only the range definition a
 * client sends and reads back survives here.</p>
 *
 * @opensearch.internal
 */
public final class RangeAggregator {

    /** The {@code ranges} array of a range aggregation. */
    public static final ParseField RANGES_FIELD = new ParseField("ranges");
    /** The {@code keyed} flag of a range aggregation. */
    public static final ParseField KEYED_FIELD = new ParseField("keyed");

    private RangeAggregator() {
    }


    /**
     * Range for the range aggregator
     *
     * @opensearch.internal
     */
    public static class Range implements Writeable, ToXContentObject {
        public static final ParseField KEY_FIELD = new ParseField("key");
        public static final ParseField FROM_FIELD = new ParseField("from");
        public static final ParseField TO_FIELD = new ParseField("to");

        protected final String key;
        protected final double from;
        protected final String fromAsStr;
        protected final double to;
        protected final String toAsStr;

        /**
         * Build the range. Generally callers should prefer
         * {@link Range#Range(String, Double, Double)} or
         * {@link Range#Range(String, String, String)}. If you
         * <strong>must</strong> call this know that consumers prefer
         * {@code from} and {@code to} parameters if they are non-null
         * and finite. Otherwise they parse from {@code fromrStr} and
         * {@code toStr}.
         */
        public Range(String key, Double from, String fromAsStr, Double to, String toAsStr) {
            this.key = key;
            this.from = from == null ? Double.NEGATIVE_INFINITY : from;
            this.fromAsStr = fromAsStr;
            this.to = to == null ? Double.POSITIVE_INFINITY : to;
            this.toAsStr = toAsStr;
        }

        public Range(String key, Double from, Double to) {
            this(key, from, null, to, null);
        }

        public Range(String key, String from, String to) {
            this(key, null, from, null, to);
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            key = in.readOptionalString();
            fromAsStr = in.readOptionalString();
            toAsStr = in.readOptionalString();
            from = in.readDouble();
            to = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(fromAsStr);
            out.writeOptionalString(toAsStr);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        @Override
        public String toString() {
            return "[" + from + " to " + to + ")";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            if (Double.isFinite(from)) {
                builder.field(FROM_FIELD.getPreferredName(), from);
            }
            if (Double.isFinite(to)) {
                builder.field(TO_FIELD.getPreferredName(), to);
            }
            if (fromAsStr != null) {
                builder.field(FROM_FIELD.getPreferredName(), fromAsStr);
            }
            if (toAsStr != null) {
                builder.field(TO_FIELD.getPreferredName(), toAsStr);
            }
            builder.endObject();
            return builder;
        }

        public static final ConstructingObjectParser<Range, Void> PARSER = new ConstructingObjectParser<>("range", arg -> {
            String key = (String) arg[0];
            Object from = arg[1];
            Object to = arg[2];
            Double fromDouble = from instanceof Number ? ((Number) from).doubleValue() : null;
            Double toDouble = to instanceof Number ? ((Number) to).doubleValue() : null;
            String fromStr = from instanceof String ? (String) from : null;
            String toStr = to instanceof String ? (String) to : null;
            return new Range(key, fromDouble, fromStr, toDouble, toStr);
        });

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), KEY_FIELD, ValueType.DOUBLE); // DOUBLE supports string and
                                                                                                            // number
            ContextParser<Void, Object> fromToParser = (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                }
                if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.doubleValue();
                }
                return null;
            };
            // DOUBLE_OR_NULL accepts String, Number, and null
            PARSER.declareField(optionalConstructorArg(), fromToParser, FROM_FIELD, ValueType.DOUBLE_OR_NULL);
            PARSER.declareField(optionalConstructorArg(), fromToParser, TO_FIELD, ValueType.DOUBLE_OR_NULL);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, from, fromAsStr, to, toAsStr);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Range other = (Range) obj;
            return Objects.equals(key, other.key)
                && Objects.equals(from, other.from)
                && Objects.equals(fromAsStr, other.fromAsStr)
                && Objects.equals(to, other.to)
                && Objects.equals(toAsStr, other.toAsStr);
        }
    }
}
