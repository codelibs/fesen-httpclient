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

import org.apache.lucene.search.Explanation;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Combine function for search
 *
 * @opensearch.internal
 */
public enum CombineFunction implements Writeable {
    /**
     * The MULTIPLY value.
     */
    MULTIPLY {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (queryScore * Math.min(funcScore, maxBoost));
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation boostExpl = Explanation.match(maxBoost, "maxBoost");
            Explanation minExpl = Explanation.match(Math.min(funcExpl.getValue().floatValue(), maxBoost), "min of:", funcExpl, boostExpl);
            return Explanation.match(
                queryExpl.getValue().floatValue() * minExpl.getValue().floatValue(),
                "function score, product of:",
                queryExpl,
                minExpl
            );
        }
    },
    /**
     * The REPLACE value.
     */
    REPLACE {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (Math.min(funcScore, maxBoost));
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation boostExpl = Explanation.match(maxBoost, "maxBoost");
            return Explanation.match(Math.min(funcExpl.getValue().floatValue(), maxBoost), "min of:", funcExpl, boostExpl);
        }

    },
    /**
     * The SUM value.
     */
    SUM {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (queryScore + Math.min(funcScore, maxBoost));
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation minExpl = Explanation.match(
                Math.min(funcExpl.getValue().floatValue(), maxBoost),
                "min of:",
                funcExpl,
                Explanation.match(maxBoost, "maxBoost")
            );
            return Explanation.match(
                Math.min(funcExpl.getValue().floatValue(), maxBoost) + queryExpl.getValue().floatValue(),
                "sum of",
                queryExpl,
                minExpl
            );
        }

    },
    /**
     * The AVG value.
     */
    AVG {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) ((Math.min(funcScore, maxBoost) + queryScore) / 2.0);
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation minExpl = Explanation.match(
                Math.min(funcExpl.getValue().floatValue(), maxBoost),
                "min of:",
                funcExpl,
                Explanation.match(maxBoost, "maxBoost")
            );
            return Explanation.match(
                (float) ((Math.min(funcExpl.getValue().floatValue(), maxBoost) + queryExpl.getValue().floatValue()) / 2.0),
                "avg of",
                queryExpl,
                minExpl
            );
        }

    },
    /**
     * The MIN value.
     */
    MIN {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (Math.min(queryScore, Math.min(funcScore, maxBoost)));
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation innerMinExpl = Explanation.match(
                Math.min(funcExpl.getValue().floatValue(), maxBoost),
                "min of:",
                funcExpl,
                Explanation.match(maxBoost, "maxBoost")
            );
            return Explanation.match(
                Math.min(Math.min(funcExpl.getValue().floatValue(), maxBoost), queryExpl.getValue().floatValue()),
                "min of",
                queryExpl,
                innerMinExpl
            );
        }

    },
    /**
     * The MAX value.
     */
    MAX {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (Math.max(queryScore, Math.min(funcScore, maxBoost)));
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation innerMinExpl = Explanation.match(
                Math.min(funcExpl.getValue().floatValue(), maxBoost),
                "min of:",
                funcExpl,
                Explanation.match(maxBoost, "maxBoost")
            );
            return Explanation.match(
                Math.max(Math.min(funcExpl.getValue().floatValue(), maxBoost), queryExpl.getValue().floatValue()),
                "max of:",
                queryExpl,
                innerMinExpl
            );
        }

    };

    /**
     * Combines this instance.
     *
     * @param queryScore the query score
     * @param funcScore the func score
     * @param maxBoost the max boost
     * @return this instance
     */
    public abstract float combine(double queryScore, double funcScore, double maxBoost);

    /**
     * Returns the explain.
     *
     * @param queryExpl the query expl
     * @param funcExpl the func expl
     * @param maxBoost the max boost
     * @return the explain
     */
    public abstract Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost);

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
    public static CombineFunction readFromStream(StreamInput in) throws IOException {
        return in.readEnum(CombineFunction.class);
    }

    /**
     * Creates an instance from string.
     *
     * @param combineFunction the combine function
     * @return the new string
     */
    public static CombineFunction fromString(String combineFunction) {
        return valueOf(combineFunction.toUpperCase(Locale.ROOT));
    }
}
