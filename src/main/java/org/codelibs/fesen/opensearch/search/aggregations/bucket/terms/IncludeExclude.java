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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.util.BitMixer;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 *
 * @opensearch.internal
 */
public class IncludeExclude implements Writeable, ToXContentFragment {
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField PARTITION_FIELD = new ParseField("partition");
    public static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");
    // Needed to add this seed for a deterministic term hashing policy
    // otherwise tests fail to get expected results and worse, shards
    // can disagree on which terms hash to the required partition.
    private static final int HASH_PARTITIONING_SEED = 31;

    // for parsing purposes only
    // TODO: move all aggs to the same package so that this stuff could be pkg-private
    public static IncludeExclude merge(IncludeExclude include, IncludeExclude exclude) {
        if (include == null) {
            return exclude;
        }
        if (exclude == null) {
            return include;
        }
        if (include.isPartitionBased()) {
            throw new IllegalArgumentException("Cannot specify any excludes when using a partition-based include");
        }
        String includeMethod = include.isRegexBased() ? "regex" : "set";
        String excludeMethod = exclude.isRegexBased() ? "regex" : "set";
        if (includeMethod.equals(excludeMethod) == false) {
            throw new IllegalArgumentException(
                "Cannot mix a " + includeMethod + "-based include with a " + excludeMethod + "-based method"
            );
        }
        if (include.isRegexBased()) {
            return new IncludeExclude(include.include, exclude.exclude);
        } else {
            return new IncludeExclude(include.includeValues, exclude.excludeValues);
        }
    }

    public static IncludeExclude parseInclude(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new IncludeExclude(parser.text(), null);
        } else if (token == XContentParser.Token.START_ARRAY) {
            return new IncludeExclude(new TreeSet<>(parseArrayToSet(parser)), null);
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            Integer partition = null, numPartitions = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (NUM_PARTITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    numPartitions = parser.intValue();
                } else if (PARTITION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    partition = parser.intValue();
                } else {
                    throw new OpenSearchParseException("Unknown parameter in Include/Exclude clause: " + currentFieldName);
                }
            }
            if (partition == null) {
                throw new IllegalArgumentException(
                    "Missing [" + PARTITION_FIELD.getPreferredName() + "] parameter for partition-based include"
                );
            }
            if (numPartitions == null) {
                throw new IllegalArgumentException(
                    "Missing [" + NUM_PARTITIONS_FIELD.getPreferredName() + "] parameter for partition-based include"
                );
            }
            return new IncludeExclude(partition, numPartitions);
        } else {
            throw new IllegalArgumentException("Unrecognized token for an include [" + token + "]");
        }
    }

    public static IncludeExclude parseExclude(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new IncludeExclude(null, parser.text());
        } else if (token == XContentParser.Token.START_ARRAY) {
            return new IncludeExclude(null, new TreeSet<>(parseArrayToSet(parser)));
        } else {
            throw new IllegalArgumentException("Unrecognized token for an exclude [" + token + "]");
        }
    }

    private final String include, exclude;
    private final SortedSet<BytesRef> includeValues, excludeValues;
    private final int incZeroBasedPartition;
    private final int incNumPartitions;

    /**
     * @param include   The string or regular expression pattern for the terms to be included
     * @param exclude   The string or regular expression pattern for the terms to be excluded
     */
    public IncludeExclude(String include, String exclude) {
        this.include = include;
        this.exclude = exclude;
        this.includeValues = null;
        this.excludeValues = null;
        this.incZeroBasedPartition = 0;
        this.incNumPartitions = 0;
    }

    /**
     * @param includeValues   The terms to be included
     * @param excludeValues   The terms to be excluded
     */
    public IncludeExclude(SortedSet<BytesRef> includeValues, SortedSet<BytesRef> excludeValues) {
        if (includeValues == null && excludeValues == null) {
            throw new IllegalArgumentException();
        }
        this.include = null;
        this.exclude = null;
        this.incZeroBasedPartition = 0;
        this.incNumPartitions = 0;
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
    }

    public IncludeExclude(int partition, int numPartitions) {
        if (partition < 0 || partition >= numPartitions) {
            throw new IllegalArgumentException("Partition must be >=0 and < numPartition which is " + numPartitions);
        }
        this.incZeroBasedPartition = partition;
        this.incNumPartitions = numPartitions;
        this.include = null;
        this.exclude = null;
        this.includeValues = null;
        this.excludeValues = null;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean regexBased = isRegexBased();
        out.writeBoolean(regexBased);
        if (regexBased) {
            out.writeOptionalString(include);
            out.writeOptionalString(exclude);
        } else {
            boolean hasIncludes = includeValues != null;
            out.writeBoolean(hasIncludes);
            if (hasIncludes) {
                out.writeVInt(includeValues.size());
                for (BytesRef value : includeValues) {
                    out.writeBytesRef(value);
                }
            }
            boolean hasExcludes = excludeValues != null;
            out.writeBoolean(hasExcludes);
            if (hasExcludes) {
                out.writeVInt(excludeValues.size());
                for (BytesRef value : excludeValues) {
                    out.writeBytesRef(value);
                }
            }
            out.writeVInt(incNumPartitions);
            out.writeVInt(incZeroBasedPartition);
        }
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(String[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (String value : values) {
                returnSet.add(new BytesRef(value));
            }
        }
        return returnSet;
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(double[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (double value : values) {
                returnSet.add(new BytesRef(String.valueOf(value)));
            }
        }
        return returnSet;
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(long[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (long value : values) {
                returnSet.add(new BytesRef(String.valueOf(value)));
            }
        }
        return returnSet;
    }

    private static Set<BytesRef> parseArrayToSet(XContentParser parser) throws IOException {
        final Set<BytesRef> set = new HashSet<>();
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new OpenSearchParseException("Missing start of array in include/exclude clause");
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (!parser.currentToken().isValue()) {
                throw new OpenSearchParseException("Array elements in include/exclude clauses should be string values");
            }
            set.add(new BytesRef(parser.text()));
        }
        return set;
    }

    public boolean isRegexBased() {
        return include != null || exclude != null;
    }

    public boolean isPartitionBased() {
        return incNumPartitions > 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (include != null) {
            builder.field(INCLUDE_FIELD.getPreferredName(), include);
        } else if (includeValues != null) {
            builder.startArray(INCLUDE_FIELD.getPreferredName());
            for (BytesRef value : includeValues) {
                builder.value(value.utf8ToString());
            }
            builder.endArray();
        } else if (isPartitionBased()) {
            builder.startObject(INCLUDE_FIELD.getPreferredName());
            builder.field(PARTITION_FIELD.getPreferredName(), incZeroBasedPartition);
            builder.field(NUM_PARTITIONS_FIELD.getPreferredName(), incNumPartitions);
            builder.endObject();
        }
        if (exclude != null) {
            builder.field(EXCLUDE_FIELD.getPreferredName(), exclude);
        } else if (excludeValues != null) {
            builder.startArray(EXCLUDE_FIELD.getPreferredName());
            for (BytesRef value : excludeValues) {
                builder.value(value.utf8ToString());
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(include, exclude, includeValues, excludeValues, incZeroBasedPartition, incNumPartitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IncludeExclude other = (IncludeExclude) obj;
        return Objects.equals(include, other.include)
            && Objects.equals(exclude, other.exclude)
            && Objects.equals(includeValues, other.includeValues)
            && Objects.equals(excludeValues, other.excludeValues)
            && Objects.equals(incZeroBasedPartition, other.incZeroBasedPartition)
            && Objects.equals(incNumPartitions, other.incNumPartitions);
    }

}
