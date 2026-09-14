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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.heuristic;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * NXY significance heuristic for significant terms agg
 *
 * @opensearch.internal
 */
public abstract class NXYSignificanceHeuristic extends SignificanceHeuristic {

    /**
     * The BACKGROUND_IS_SUPERSET constant.
     */
    protected static final ParseField BACKGROUND_IS_SUPERSET = new ParseField("background_is_superset");

    /**
     * The INCLUDE_NEGATIVES_FIELD constant.
     */
    protected static final ParseField INCLUDE_NEGATIVES_FIELD = new ParseField("include_negatives");

    /**
     * The SCORE_ERROR_MESSAGE constant.
     */
    protected static final String SCORE_ERROR_MESSAGE = ", does your background filter not include all documents in the bucket? "
        + "If so and it is intentional, set \""
        + BACKGROUND_IS_SUPERSET.getPreferredName()
        + "\": false";

    /**
     * The background is superset.
     */
    protected final boolean backgroundIsSuperset;

    /**
     * Some heuristics do not differentiate between terms that are descriptive for subset or for
     * the background without the subset. We might want to filter out the terms that are appear much less often
     * in the subset than in the background without the subset.
     */
    protected final boolean includeNegatives;

    /**
     * Creates a new NXYSignificanceHeuristic.
     *
     * @param includeNegatives the include negatives
     * @param backgroundIsSuperset the background is superset
     */
    protected NXYSignificanceHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
        this.includeNegatives = includeNegatives;
        this.backgroundIsSuperset = backgroundIsSuperset;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeNegatives);
        out.writeBoolean(backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NXYSignificanceHeuristic other = (NXYSignificanceHeuristic) obj;
        if (backgroundIsSuperset != other.backgroundIsSuperset) return false;
        if (includeNegatives != other.includeNegatives) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = (includeNegatives ? 1 : 0);
        result = 31 * result + (backgroundIsSuperset ? 1 : 0);
        return result;
    }

    /**
     * Frequencies for an NXY significance heuristic
     *
     * @opensearch.internal
     */
    protected static class Frequencies {
        /**
         * Creates a new Frequencies.
         */
        protected Frequencies() {
        }

        double N00, N01, N10, N11, N0_, N1_, N_0, N_1, N;
    }

    /**
     * Computes the nxys.
     *
     * @param subsetFreq the subset freq
     * @param subsetSize the subset size
     * @param supersetFreq the superset freq
     * @param supersetSize the superset size
     * @param scoreFunctionName the score function name
     * @return the nxys
     */
    protected Frequencies computeNxys(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencies(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        Frequencies frequencies = new Frequencies();
        if (backgroundIsSuperset) {
            // documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq - (subsetSize - subsetFreq);
            // documents in class and do not contain term
            frequencies.N01 = (subsetSize - subsetFreq);
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq - subsetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            // documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq;
            // documents that contain term
            frequencies.N1_ = supersetFreq;
            // documents that are not in class
            frequencies.N_0 = supersetSize - subsetSize;
            // documents that are in class
            frequencies.N_1 = subsetSize;
            // all docs
            frequencies.N = supersetSize;
        } else {
            // documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq;
            // documents in class and do not contain term
            frequencies.N01 = subsetSize - subsetFreq;
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            // documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq + subsetSize - subsetFreq;
            // documents that contain term
            frequencies.N1_ = supersetFreq + subsetFreq;
            // documents that are not in class
            frequencies.N_0 = supersetSize;
            // documents that are in class
            frequencies.N_1 = subsetSize;
            // all docs
            frequencies.N = supersetSize + subsetSize;
        }
        return frequencies;
    }

    /**
     * Checks the frequencies.
     *
     * @param subsetFreq the subset freq
     * @param subsetSize the subset size
     * @param supersetFreq the superset freq
     * @param supersetSize the superset size
     * @param scoreFunctionName the score function name
     */
    protected void checkFrequencies(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        if (backgroundIsSuperset) {
            if (subsetFreq > supersetFreq) {
                throw new IllegalArgumentException("subsetFreq > supersetFreq" + SCORE_ERROR_MESSAGE);
            }
            if (subsetSize > supersetSize) {
                throw new IllegalArgumentException("subsetSize > supersetSize" + SCORE_ERROR_MESSAGE);
            }
            if (supersetFreq - subsetFreq > supersetSize - subsetSize) {
                throw new IllegalArgumentException("supersetFreq - subsetFreq > supersetSize - subsetSize" + SCORE_ERROR_MESSAGE);
            }
        }
    }

    /**
     * Builds this instance.
     *
     * @param builder the content builder
     * @throws IOException if an I/O error occurs
     */
    protected void build(XContentBuilder builder) throws IOException {
        builder.field(INCLUDE_NEGATIVES_FIELD.getPreferredName(), includeNegatives)
            .field(BACKGROUND_IS_SUPERSET.getPreferredName(), backgroundIsSuperset);
    }

    /**
     * Set up and {@linkplain ConstructingObjectParser} to accept the standard arguments for an {@linkplain NXYSignificanceHeuristic}.
     *
     * @param parser the parser
     */
    protected static void declareParseFields(ConstructingObjectParser<? extends NXYSignificanceHeuristic, ?> parser) {
        parser.declareBoolean(optionalConstructorArg(), INCLUDE_NEGATIVES_FIELD);
        parser.declareBoolean(optionalConstructorArg(), BACKGROUND_IS_SUPERSET);
    }

    /**
     * Adapt a standard two argument ctor into one that consumes a {@linkplain ConstructingObjectParser}'s fields.
     *
     * @param <T> the element type
     * @param ctor the ctor
     * @return the new from parsed args
     */
    protected static <T> Function<Object[], T> buildFromParsedArgs(BiFunction<Boolean, Boolean, T> ctor) {
        return args -> {
            boolean includeNegatives = args[0] == null ? false : (boolean) args[0];
            boolean backgroundIsSuperset = args[1] == null ? true : (boolean) args[1];
            return ctor.apply(includeNegatives, backgroundIsSuperset);
        };
    }
}
