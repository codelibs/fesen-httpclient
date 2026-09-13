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

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.index.query.SimpleQueryStringBuilder;

import java.util.Objects;

/**
 * Namespace for the simple_query_string settings a client sends over the wire.
 *
 * <p>Parsing the query text into a Lucene query is a node-side concern and is not carried over;
 * only the settings object the builder serialises survives here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SimpleQueryStringQueryParser {

    private SimpleQueryStringQueryParser() {
    }

    public static class Settings {
        /** Specifies whether lenient query parsing should be used. */
        private boolean lenient = SimpleQueryStringBuilder.DEFAULT_LENIENT;
        /** Specifies whether wildcards should be analyzed. */
        private boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        /** Specifies a suffix, if any, to apply to field names for phrase matching. */
        private String quoteFieldSuffix = null;
        /** Whether phrase queries should be automatically generated for multi terms synonyms. */
        private boolean autoGenerateSynonymsPhraseQuery = true;
        /** Prefix length in fuzzy queries.*/
        private int fuzzyPrefixLength = SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        /** The number of terms fuzzy queries will expand to.*/
        private int fuzzyMaxExpansions = SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        /** Whether transpositions are supported in fuzzy queries.*/
        private boolean fuzzyTranspositions = SimpleQueryStringBuilder.DEFAULT_FUZZY_TRANSPOSITIONS;

        /**
         * Generates default {@link Settings} object (uses ROOT locale, does
         * lowercase terms, no lenient parsing, no wildcard analysis).
         * */
        public Settings() {}

        public Settings(Settings other) {
            this.lenient = other.lenient;
            this.analyzeWildcard = other.analyzeWildcard;
            this.quoteFieldSuffix = other.quoteFieldSuffix;
            this.autoGenerateSynonymsPhraseQuery = other.autoGenerateSynonymsPhraseQuery;
            this.fuzzyPrefixLength = other.fuzzyPrefixLength;
            this.fuzzyMaxExpansions = other.fuzzyMaxExpansions;
            this.fuzzyTranspositions = other.fuzzyTranspositions;
        }

        /** Specifies whether to use lenient parsing, defaults to false. */
        public void lenient(boolean lenient) {
            this.lenient = lenient;
        }

        /** Returns whether to use lenient parsing. */
        public boolean lenient() {
            return this.lenient;
        }

        /** Specifies whether to analyze wildcards. Defaults to false if unset. */
        public void analyzeWildcard(boolean analyzeWildcard) {
            this.analyzeWildcard = analyzeWildcard;
        }

        /** Returns whether to analyze wildcards. */
        public boolean analyzeWildcard() {
            return analyzeWildcard;
        }

        /**
         * Set the suffix to append to field names for phrase matching.
         */
        public void quoteFieldSuffix(String suffix) {
            this.quoteFieldSuffix = suffix;
        }

        /**
         * Return the suffix to append for phrase matching, or {@code null} if
         * no suffix should be appended.
         */
        public String quoteFieldSuffix() {
            return quoteFieldSuffix;
        }

        public void autoGenerateSynonymsPhraseQuery(boolean value) {
            this.autoGenerateSynonymsPhraseQuery = value;
        }

        /**
         * Whether phrase queries should be automatically generated for multi terms synonyms.
         * Defaults to {@code true}.
         */
        public boolean autoGenerateSynonymsPhraseQuery() {
            return autoGenerateSynonymsPhraseQuery;
        }

        public int fuzzyPrefixLength() {
            return fuzzyPrefixLength;
        }

        public void fuzzyPrefixLength(int fuzzyPrefixLength) {
            this.fuzzyPrefixLength = fuzzyPrefixLength;
        }

        public int fuzzyMaxExpansions() {
            return fuzzyMaxExpansions;
        }

        public void fuzzyMaxExpansions(int fuzzyMaxExpansions) {
            this.fuzzyMaxExpansions = fuzzyMaxExpansions;
        }

        public boolean fuzzyTranspositions() {
            return fuzzyTranspositions;
        }

        public void fuzzyTranspositions(boolean fuzzyTranspositions) {
            this.fuzzyTranspositions = fuzzyTranspositions;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                lenient,
                analyzeWildcard,
                quoteFieldSuffix,
                autoGenerateSynonymsPhraseQuery,
                fuzzyPrefixLength,
                fuzzyMaxExpansions,
                fuzzyTranspositions
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Settings other = (Settings) obj;
            return Objects.equals(lenient, other.lenient)
                && Objects.equals(analyzeWildcard, other.analyzeWildcard)
                && Objects.equals(quoteFieldSuffix, other.quoteFieldSuffix)
                && Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery)
                && Objects.equals(fuzzyPrefixLength, other.fuzzyPrefixLength)
                && Objects.equals(fuzzyMaxExpansions, other.fuzzyMaxExpansions)
                && Objects.equals(fuzzyTranspositions, other.fuzzyTranspositions);
        }
    }
}
