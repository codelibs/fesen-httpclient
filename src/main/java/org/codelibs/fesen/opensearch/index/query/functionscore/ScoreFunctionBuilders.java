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

package org.codelibs.fesen.opensearch.index.query.functionscore;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.script.ScriptType;

import static java.util.Collections.emptyMap;

/**
 * Static method aliases for constructors of known {@link ScoreFunctionBuilder}s.
 *
 * @opensearch.internal
 */
public class ScoreFunctionBuilders {
    /**
     * Creates a new ScoreFunctionBuilders.
     */
    public ScoreFunctionBuilders() {
    }

    /**
     * Returns the random function.
     *
     * @return the random function
     */
    public static RandomScoreFunctionBuilder randomFunction() {
        return randomFunction(null);
    }

    /**
     * Returns the weight factor function.
     *
     * @param weight the weight
     * @return the weight factor function
     */
    public static WeightBuilder weightFactorFunction(float weight) {
        return weightFactorFunction(weight, null);
    }

    /**
     * Returns the field value factor function.
     *
     * @param fieldName the field name
     * @return the field value factor function
     */
    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName) {
        return fieldValueFactorFunction(fieldName, null);
    }

    /**
     * Returns the random function.
     *
     * @param functionName the function name
     * @return the random function
     */
    public static RandomScoreFunctionBuilder randomFunction(@Nullable String functionName) {
        return new RandomScoreFunctionBuilder(functionName);
    }

    /**
     * Returns the weight factor function.
     *
     * @param weight the weight
     * @param functionName the function name
     * @return the weight factor function
     */
    public static WeightBuilder weightFactorFunction(float weight, @Nullable String functionName) {
        return (WeightBuilder) (new WeightBuilder(functionName).setWeight(weight));
    }

    /**
     * Returns the field value factor function.
     *
     * @param fieldName the field name
     * @param functionName the function name
     * @return the field value factor function
     */
    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName, @Nullable String functionName) {
        return new FieldValueFactorFunctionBuilder(fieldName, functionName);
    }
}
