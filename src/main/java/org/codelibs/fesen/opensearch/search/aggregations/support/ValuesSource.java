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
package org.codelibs.fesen.opensearch.search.aggregations.support;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * A source of values an aggregation runs over.
 *
 * <p>Reading values out of a segment is a node-side concern and is not carried over; only the type
 * hierarchy the aggregation builders are generic over survives here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ValuesSource {

    /** A source of numeric values. */
    public abstract static class Numeric extends ValuesSource {
    }

    /** A source of geo-point values. */
    public abstract static class GeoPoint extends ValuesSource {
    }
}
