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

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

/**
 * Statistics over a set of values (either aggregated over field data or scripts)
 *
 * @opensearch.internal
 */
public interface ExtendedStats extends Stats {

    /**
     * The sum of the squares of the collected values.
     *
     * @return the sum of squares
     */
    double getSumOfSquares();

    /**
     * The population variance of the collected values.
     *
     * @return the variance
     */
    double getVariance();

    /**
     * The population variance of the collected values.
     *
     * @return the variance population
     */
    double getVariancePopulation();

    /**
     * The sampling variance of the collected values.
     *
     * @return the variance sampling
     */
    double getVarianceSampling();

    /**
     * The population standard deviation of the collected values.
     *
     * @return the std deviation
     */
    double getStdDeviation();

    /**
     * The population standard deviation of the collected values.
     *
     * @return the std deviation population
     */
    double getStdDeviationPopulation();

    /**
     * The sampling standard deviation of the collected values.
     *
     * @return the std deviation sampling
     */
    double getStdDeviationSampling();

    /**
     * The upper or lower bounds of the stdDeviation
     *
     * @param bound the bound
     * @return the std deviation bound
     */
    double getStdDeviationBound(Bounds bound);

    /**
     * The population standard deviation of the collected values as a String.
     *
     * @return the std deviation as string
     */
    String getStdDeviationAsString();

    /**
     * The population standard deviation of the collected values as a String.
     *
     * @return the std deviation population as string
     */
    String getStdDeviationPopulationAsString();

    /**
     * The sampling standard deviation of the collected values as a String.
     *
     * @return the std deviation sampling as string
     */
    String getStdDeviationSamplingAsString();

    /**
     * The upper or lower bounds of stdDev of the collected values as a String.
     *
     * @param bound the bound
     * @return the std deviation bound as string
     */
    String getStdDeviationBoundAsString(Bounds bound);

    /**
     * The sum of the squares of the collected values as a String.
     *
     * @return the sum of squares as string
     */
    String getSumOfSquaresAsString();

    /**
     * The population variance of the collected values as a String.
     *
     * @return the variance as string
     */
    String getVarianceAsString();

    /**
     * The population variance of the collected values as a String.
     *
     * @return the variance population as string
     */
    String getVariancePopulationAsString();

    /**
     * The sampling variance of the collected values as a String.
     *
     * @return the variance sampling as string
     */
    String getVarianceSamplingAsString();

    /**
     * The bounds of the extended stats
     *
     * @opensearch.internal
     */
    enum Bounds {
        /**
         * The UPPER value.
         */
        UPPER,
        /**
         * The LOWER value.
         */
        LOWER,
        /**
         * The UPPER_POPULATION value.
         */
        UPPER_POPULATION,
        /**
         * The LOWER_POPULATION value.
         */
        LOWER_POPULATION,
        /**
         * The UPPER_SAMPLING value.
         */
        UPPER_SAMPLING,
        /**
         * The lower sampling.
         */
        LOWER_SAMPLING
    }

}
