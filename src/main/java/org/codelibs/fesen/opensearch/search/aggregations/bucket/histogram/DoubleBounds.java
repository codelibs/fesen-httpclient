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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.InstantiatingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represent hard_bounds and extended_bounds in histogram aggregations.
 * <p>
 * This class is similar to {@link LongBounds} used in date histograms, but is using longs to store data. LongBounds and DoubleBounds are
 * not used interchangeably and therefore don't share any common interfaces except for serialization.
 *
 * @opensearch.internal
 */

public class DoubleBounds implements ToXContentFragment, Writeable {
    static final ParseField MIN_FIELD = new ParseField("min");
    static final ParseField MAX_FIELD = new ParseField("max");
    static final InstantiatingObjectParser<DoubleBounds, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<DoubleBounds, Void> parser = InstantiatingObjectParser.builder(
            "double_bounds",
            false,
            DoubleBounds.class
        );
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.doubleValue(),
            MIN_FIELD,
            ObjectParser.ValueType.DOUBLE_OR_NULL
        );
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.doubleValue(),
            MAX_FIELD,
            ObjectParser.ValueType.DOUBLE_OR_NULL
        );
        PARSER = parser.build();
    }

    /**
     * Min value
     */
    private final Double min;

    /**
     * Max value
     */
    private final Double max;

    /**
     * Construct with bounds.
     *
     * @param min the min
     * @param max the max
     */
    public DoubleBounds(Double min, Double max) {
        if (min != null && Double.isFinite(min) == false) {
            throw new IllegalArgumentException("min bound must be finite, got: " + min);
        }
        if (max != null && Double.isFinite(max) == false) {
            throw new IllegalArgumentException("max bound must be finite, got: " + max);
        }
        if (max != null && min != null && max < min) {
            throw new IllegalArgumentException("max bound [" + max + "] must be greater than min bound [" + min + "]");
        }
        this.min = min;
        this.max = max;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public DoubleBounds(StreamInput in) throws IOException {
        min = in.readOptionalDouble();
        max = in.readOptionalDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(min);
        out.writeOptionalDouble(max);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (min != null) {
            builder.field(MIN_FIELD.getPreferredName(), min);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DoubleBounds other = (DoubleBounds) obj;
        return Objects.equals(min, other.min) && Objects.equals(max, other.max);
    }

    /**
     * returns bounds min if it is defined or POSITIVE_INFINITY otherwise
     *
     * @param bounds the bounds
     * @return the effective min
     */
    public static double getEffectiveMin(DoubleBounds bounds) {
        return bounds == null || bounds.min == null ? Double.POSITIVE_INFINITY : bounds.min;
    }

    /**
     * returns bounds max if it is defined or NEGATIVE_INFINITY otherwise
     *
     * @param bounds the bounds
     * @return the effective max
     */
    public static Double getEffectiveMax(DoubleBounds bounds) {
        return bounds == null || bounds.max == null ? Double.NEGATIVE_INFINITY : bounds.max;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (min != null) {
            b.append(min);
        }
        b.append("--");
        if (max != null) {
            b.append(max);
        }
        return b.toString();
    }
}
