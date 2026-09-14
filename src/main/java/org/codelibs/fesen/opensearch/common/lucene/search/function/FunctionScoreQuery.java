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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
package org.codelibs.fesen.opensearch.common.lucene.search.function;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Namespace for the function_score request types shared by the query builder and the wire format.
 *
 * <p>Scoring documents is a node-side concern and is not carried over; only the score mode and the
 * default max boost a client sends survive here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class FunctionScoreQuery {

    /** The default upper bound a function score is clamped to. */
    public static final float DEFAULT_MAX_BOOST = Float.MAX_VALUE;

    private FunctionScoreQuery() {
    }

    /**
     * The mode of the score
     *
     * @opensearch.internal
     */
    public enum ScoreMode implements Writeable {
        /**
         * The FIRST value.
         */
        FIRST,
        /**
         * The AVG value.
         */
        AVG,
        /**
         * The MAX value.
         */
        MAX,
        /**
         * The SUM value.
         */
        SUM,
        /**
         * The MIN value.
         */
        MIN,
        /**
         * The MULTIPLY value.
         */
        MULTIPLY;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        /**
         * Reads the from stream.
         *
         * @param in the input to read from
         * @return the from stream
         * @throws IOException if an I/O error occurs
         */
        public static ScoreMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(ScoreMode.class);
        }

        /**
         * Creates an instance from string.
         *
         * @param scoreMode the score mode
         * @return the new string
         */
        public static ScoreMode fromString(String scoreMode) {
            return valueOf(scoreMode.toUpperCase(Locale.ROOT));
        }
    }
}
