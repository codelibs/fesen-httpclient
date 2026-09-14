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

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.common.network.InetAddresses;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser.Token;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.codelibs.fesen.opensearch.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.opensearch.search.aggregations.support.CoreValuesSourceType;

/**
 * Aggregation Builder for ip_range agg
 *
 * @opensearch.internal
 */
public final class IpRangeAggregationBuilder extends ValuesSourceAggregationBuilder<IpRangeAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "ip_range";
    private static final ParseField MASK_FIELD = new ParseField("mask");

    /**
     * The PARSER constant.
     */
    public static final ObjectParser<IpRangeAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        IpRangeAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);

        PARSER.declareBoolean(IpRangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges)
                agg.addRange(range);
        }, (p, c) -> IpRangeAggregationBuilder.parseRange(p), RangeAggregator.RANGES_FIELD);
    }

    private static Range parseRange(XContentParser parser) throws IOException {
        String key = null;
        String from = null;
        String to = null;
        String mask = null;

        if (parser.currentToken() != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[ranges] must contain objects, but hit a " + parser.currentToken());
        }
        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                continue;
            }
            if (RangeAggregator.Range.KEY_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                key = parser.text();
            } else if (RangeAggregator.Range.FROM_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                from = parser.textOrNull();
            } else if (RangeAggregator.Range.TO_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                to = parser.textOrNull();
            } else if (MASK_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                mask = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected ip range parameter: [" + parser.currentName() + "]");
            }
        }
        if (mask != null) {
            if (key == null) {
                key = mask;
            }
            return new Range(key, mask);
        } else {
            return new Range(key, from, to);
        }
    }

    /**
     * Range for an IP range
     *
     * @opensearch.internal
     */
    public static class Range implements ToXContentObject {

        private final String key;
        private final String from;
        private final String to;

        Range(String key, String from, String to) {
            if (from != null) {
                InetAddresses.forString(from);
            }
            if (to != null) {
                InetAddresses.forString(to);
            }
            this.key = key;
            this.from = from;
            this.to = to;
        }

        Range(String key, String mask) {
            final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(mask);
            final InetAddress address = cidr.v1();
            final int prefixLength = cidr.v2();
            // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
            byte lower[] = address.getAddress();
            byte upper[] = address.getAddress();
            for (int i = prefixLength; i < 8 * lower.length; i++) {
                int m = 1 << (7 - (i & 7));
                lower[i >> 3] &= ~m;
                upper[i >> 3] |= m;
            }
            this.key = key;
            try {
                InetAddress fromAddress = InetAddress.getByAddress(lower);
                if (fromAddress.equals(InetAddressPoint.MIN_VALUE)) {
                    this.from = null;
                } else {
                    this.from = InetAddresses.toAddrString(fromAddress);
                }
                InetAddress inclusiveToAddress = InetAddress.getByAddress(upper);
                if (inclusiveToAddress.equals(InetAddressPoint.MAX_VALUE)) {
                    this.to = null;
                } else {
                    this.to = InetAddresses.toAddrString(InetAddressPoint.nextUp(inclusiveToAddress));
                }
            } catch (UnknownHostException bogus) {
                throw new AssertionError(bogus);
            }
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(from);
            out.writeOptionalString(to);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Range that = (Range) obj;
            return Objects.equals(key, that.key) && Objects.equals(from, that.from) && Objects.equals(to, that.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, from, to);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(RangeAggregator.Range.KEY_FIELD.getPreferredName(), key);
            }
            if (from != null) {
                builder.field(RangeAggregator.Range.FROM_FIELD.getPreferredName(), from);
            }
            if (to != null) {
                builder.field(RangeAggregator.Range.TO_FIELD.getPreferredName(), to);
            }
            builder.endObject();
            return builder;
        }
    }

    private boolean keyed = false;
    private List<Range> ranges = new ArrayList<>();

    /**
     * Creates a new IpRangeAggregationBuilder.
     *
     * @param name the name
     */
    public IpRangeAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Creates a new IpRangeAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected IpRangeAggregationBuilder(IpRangeAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.ranges = new ArrayList<>(clone.ranges);
        this.keyed = clone.keyed;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new IpRangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Returns the keyed.
     *
     * @param keyed the keyed
     * @return the keyed
     */
    public IpRangeAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /**
     * Returns the keyed.
     *
     * @return the keyed
     */
    public boolean keyed() {
        return keyed;
    }

    /**
     * Get the current list or ranges that are configured on this aggregation.
     *
     * @return the ranges
     */
    public List<Range> getRanges() {
        return Collections.unmodifiableList(ranges);
    }

    /**
     * Add a new {@link Range} to this aggregation.
     *
     * @param range the range
     * @return this instance
     */
    public IpRangeAggregationBuilder addRange(Range range) {
        ranges.add(range);
        return this;
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @param to
     *            the upper bound on the distances, exclusive
     * @return this instance
     */
    public IpRangeAggregationBuilder addRange(String key, String from, String to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Add a new range to this aggregation using the CIDR notation.
     *
     * @param key the key
     * @param mask the mask
     * @return this instance
     */
    public IpRangeAggregationBuilder addMaskRange(String key, String mask) {
        return addRange(new Range(key, mask));
    }

    /**
     * Same as {@link #addMaskRange(String, String)} but uses the mask itself as
     * a key.
     *
     * @param mask the mask
     * @return this instance
     */
    public IpRangeAggregationBuilder addMaskRange(String mask) {
        return addRange(new Range(mask, mask));
    }

    /**
     * Same as {@link #addRange(String, String, String)} but the key will be
     * automatically generated.
     *
     * @param from the offset
     * @param to the target
     * @return this instance
     */
    public IpRangeAggregationBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * lower bound.
     *
     * @param key the key
     * @param to the target
     * @return this instance
     */
    public IpRangeAggregationBuilder addUnboundedTo(String key, String to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, String)} but the key will be
     * generated automatically.
     *
     * @param to the target
     * @return this instance
     */
    public IpRangeAggregationBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * upper bound.
     *
     * @param key the key
     * @param from the offset
     * @return this instance
     */
    public IpRangeAggregationBuilder addUnboundedFrom(String key, String from) {
        addRange(new Range(key, from, null));
        return this;
    }

    @Override
    public IpRangeAggregationBuilder script(Script script) {
        throw new IllegalArgumentException("[ip_range] does not support scripts");
    }

    /**
     * Same as {@link #addUnboundedFrom(String, String)} but the key will be
     * generated automatically.
     *
     * @param from the offset
     * @return this instance
     */
    public IpRangeAggregationBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.IP;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
    }

    private static BytesRef toBytesRef(String ip) {
        if (ip == null) {
            return null;
        }
        InetAddress address = InetAddresses.forString(ip);
        byte[] bytes = InetAddressPoint.encode(address);
        return new BytesRef(bytes);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, ranges);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        IpRangeAggregationBuilder that = (IpRangeAggregationBuilder) obj;
        return keyed == that.keyed && ranges.equals(that.ranges);
    }
}
