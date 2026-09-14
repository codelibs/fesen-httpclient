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

package org.codelibs.fesen.opensearch.search.aggregations;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;

import java.io.IOException;

/**
 * Namespace for the aggregator-side constants that are part of the request API.
 *
 * <p>Only the pieces a client needs to build and serialise a request survive here: running an
 * aggregation is a node-side concern, so the aggregator contract itself is not carried over.</p>
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public final class Aggregator {

    private Aggregator() {
    }

    /**
     * Aggregation mode for sub aggregations.
     *
     * @opensearch.internal
     */
    public enum SubAggCollectionMode implements Writeable {

        /**
         * Creates buckets and delegates to child aggregators in a single pass over
         * the matching documents
         */
        DEPTH_FIRST(new ParseField("depth_first")),

        /**
         * Creates buckets for all matching docs and then prunes to top-scoring buckets
         * before a second pass over the data when child aggregators are called
         * but only for docs from the top-scoring buckets
         */
        BREADTH_FIRST(new ParseField("breadth_first"));

        /**
         * The KEY constant.
         */
        public static final ParseField KEY = new ParseField("collect_mode");

        private final ParseField parseField;

        SubAggCollectionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        /**
         * Parses the field.
         *
         * @return this instance
         */
        public ParseField parseField() {
            return parseField;
        }

        /**
         * Parses this instance.
         *
         * @param value the value
         * @param deprecationHandler the deprecation handler
         * @return this instance
         */
        public static SubAggCollectionMode parse(String value, DeprecationHandler deprecationHandler) {
            SubAggCollectionMode[] modes = SubAggCollectionMode.values();
            for (SubAggCollectionMode mode : modes) {
                if (mode.parseField.match(value, deprecationHandler)) {
                    return mode;
                }
            }
            throw new OpenSearchParseException("no [{}] found for value [{}]", KEY.getPreferredName(), value);
        }

        /**
         * Reads the from stream.
         *
         * @param in the input to read from
         * @return the from stream
         * @throws IOException if an I/O error occurs
         */
        public static SubAggCollectionMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(SubAggCollectionMode.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
}
