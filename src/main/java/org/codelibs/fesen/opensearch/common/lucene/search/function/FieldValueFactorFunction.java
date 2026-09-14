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
 * Namespace for the field_value_factor request types shared by the builder and the wire format.
 *
 * <p>Applying the factor to a document's field value is a node-side concern and is not carried over;
 * only the modifier a client sends survives here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class FieldValueFactorFunction {

    private FieldValueFactorFunction() {
    }

    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     *
     * @opensearch.internal
     */
    public enum Modifier implements Writeable {
        /**
         * The NONE value.
         */
        NONE {
            @Override
            public double apply(double n) {
                return n;
            }
        },
        /**
         * The LOG value.
         */
        LOG {
            @Override
            public double apply(double n) {
                return Math.log10(n);
            }
        },
        /**
         * The LOG1P value.
         */
        LOG1P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 1);
            }
        },
        /**
         * The LOG2P value.
         */
        LOG2P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 2);
            }
        },
        /**
         * The LN value.
         */
        LN {
            @Override
            public double apply(double n) {
                return Math.log(n);
            }
        },
        /**
         * The LN1P value.
         */
        LN1P {
            @Override
            public double apply(double n) {
                return Math.log1p(n);
            }
        },
        /**
         * The LN2P value.
         */
        LN2P {
            @Override
            public double apply(double n) {
                return Math.log1p(n + 1);
            }
        },
        /**
         * The SQUARE value.
         */
        SQUARE {
            @Override
            public double apply(double n) {
                return Math.pow(n, 2);
            }
        },
        /**
         * The SQRT value.
         */
        SQRT {
            @Override
            public double apply(double n) {
                return Math.sqrt(n);
            }
        },
        /**
         * The RECIPROCAL value.
         */
        RECIPROCAL {
            @Override
            public double apply(double n) {
                return 1.0 / n;
            }
        };

        /**
         * Applies this instance to the given input.
         *
         * @param n the n
         * @return this instance
         */
        public abstract double apply(double n);

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
        public static Modifier readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Modifier.class);
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
}
