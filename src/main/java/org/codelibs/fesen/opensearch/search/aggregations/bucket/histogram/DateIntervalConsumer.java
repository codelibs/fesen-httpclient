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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram;

/**
 * A shared interface for aggregations that parse and use "interval" parameters.
 * <p>
 * Provides definitions for the new fixed and calendar intervals, and deprecated
 * defintions for the old interval/dateHisto interval parameters
 *
 * @param <T> the element type
 * @opensearch.internal
 */
public interface DateIntervalConsumer<T> {
    /**
     * Returns the interval.
     *
     * @param interval the interval
     * @return the interval
     */
    @Deprecated
    T interval(long interval);

    /**
     * Returns the date histogram interval.
     *
     * @param dateHistogramInterval the date histogram interval
     * @return the date histogram interval
     */
    @Deprecated
    T dateHistogramInterval(DateHistogramInterval dateHistogramInterval);

    /**
     * Returns the calendar interval.
     *
     * @param interval the interval
     * @return the calendar interval
     */
    T calendarInterval(DateHistogramInterval interval);

    /**
     * Returns the fixed interval.
     *
     * @param interval the interval
     * @return the fixed interval
     */
    T fixedInterval(DateHistogramInterval interval);

    /**
     * Returns the interval.
     *
     * @return the interval
     */
    @Deprecated
    long interval();

    /**
     * Returns the date histogram interval.
     *
     * @return the date histogram interval
     */
    @Deprecated
    DateHistogramInterval dateHistogramInterval();
}
