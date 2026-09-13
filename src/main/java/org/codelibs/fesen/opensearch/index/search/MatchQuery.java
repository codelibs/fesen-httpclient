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
package org.codelibs.fesen.opensearch.index.search;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Namespace for the match-query request types and defaults shared by the match query builders.
 *
 * <p>Turning a match query into a Lucene query is a node-side concern and is not carried over; only
 * the enums and defaults a client sends over the wire survive here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class MatchQuery {

    /** The default phrase slop. */
    public static final int DEFAULT_PHRASE_SLOP = 0;

    /** Whether format-based errors are ignored by default. */
    public static final boolean DEFAULT_LENIENCY = false;

    /** What to do by default when analysis produces no terms. */
    public static final ZeroTermsQuery DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQuery.NONE;

    private MatchQuery() {
    }

    /**
     * The shape of the Lucene query a match query analyses to.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Type implements Writeable {
        /**
         * The text is analyzed and terms are added to a boolean query.
         */
        BOOLEAN(0),
        /**
         * The text is analyzed and used as a phrase query.
         */
        PHRASE(1),
        /**
         * The text is analyzed and used in a phrase query, with the last term acting as a prefix.
         */
        PHRASE_PREFIX(2),
        /**
         * The text is analyzed, terms are added to a boolean query with the last term acting as a prefix.
         */
        BOOLEAN_PREFIX(3);

        private final int ordinal;

        Type(int ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    /**
     * What a match query matches when analysis produces no terms.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum ZeroTermsQuery implements Writeable {
        NONE(0),
        ALL(1),
        // this is used internally to make sure that query_string and simple_query_string
        // ignores query part that removes all tokens.
        NULL(2);

        private final int ordinal;

        ZeroTermsQuery(int ordinal) {
            this.ordinal = ordinal;
        }

        /**
         * Reads a zero-terms behaviour off the wire.
         *
         * @param in the stream to read from
         * @return the zero-terms behaviour
         * @throws IOException if reading fails
         */
        public static ZeroTermsQuery readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (ZeroTermsQuery zeroTermsQuery : ZeroTermsQuery.values()) {
                if (zeroTermsQuery.ordinal == ord) {
                    return zeroTermsQuery;
                }
            }
            throw new OpenSearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }
}
