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

package org.codelibs.fesen.opensearch.search.aggregations.pipeline;

import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.XContentLocation;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationExecutionException;
import org.codelibs.fesen.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.codelibs.fesen.opensearch.search.aggregations.InvalidAggregationPathException;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.codelibs.fesen.opensearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set of static helpers to simplify working with aggregation buckets, in
 * particular providing utilities that help pipeline aggregations.
 *
 * @opensearch.internal
 */
public class BucketHelpers {

    /**
     * A gap policy determines how "holes" in a set of buckets should be handled.  For example,
     * a date_histogram might have empty buckets due to no data existing for that time interval.
     * This can cause problems for operations like a derivative, which relies on a continuous
     * function.
     * <p>
     * "insert_zeros": empty buckets will be filled with zeros for all metrics
     * "skip": empty buckets will simply be ignored
     *
     * @opensearch.internal
     */
    public enum GapPolicy implements Writeable {
        INSERT_ZEROS((byte) 0, "insert_zeros"),
        SKIP((byte) 1, "skip");

        /**
         * Parse a string GapPolicy into the byte enum
         *
         * @param text
         *            GapPolicy in string format (e.g. "ignore")
         * @return GapPolicy enum
         */
        public static GapPolicy parse(String text, XContentLocation tokenLocation) {
            GapPolicy result = null;
            for (GapPolicy policy : values()) {
                if (policy.parseField.match(text, LoggingDeprecationHandler.INSTANCE)) {
                    if (result == null) {
                        result = policy;
                    } else {
                        throw new IllegalStateException(
                            "Text can be parsed to 2 different gap policies: text=["
                                + text
                                + "], "
                                + "policies="
                                + Arrays.asList(result, policy)
                        );
                    }
                }
            }
            if (result == null) {
                final List<String> validNames = new ArrayList<>();
                for (GapPolicy policy : values()) {
                    validNames.add(policy.getName());
                }
                throw new ParsingException(tokenLocation, "Invalid gap policy: [" + text + "], accepted values: " + validNames);
            }
            return result;
        }

        private final byte id;
        private final ParseField parseField;

        GapPolicy(byte id, String name) {
            this.id = id;
            this.parseField = new ParseField(name);
        }

        /**
         * Serialize the GapPolicy to the output stream
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        /**
         * Deserialize the GapPolicy from the input stream
         *
         * @return    GapPolicy Enum
         */
        public static GapPolicy readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (GapPolicy gapPolicy : values()) {
                if (id == gapPolicy.id) {
                    return gapPolicy;
                }
            }
            throw new IllegalStateException("Unknown GapPolicy with id [" + id + "]");
        }

        /**
         * Return the english-formatted name of the GapPolicy
         *
         * @return English representation of GapPolicy
         */
        public String getName() {
            return parseField.getPreferredName();
        }
    }
}
