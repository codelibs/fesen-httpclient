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

package org.codelibs.fesen.opensearch.search.internal;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * The client-side remnant of the node's search context: the request-level constants a client needs
 * to build and interpret a search request. Executing a search is a node-side concern and is not
 * carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SearchContext {

    /** Do not terminate the query early. */
    public static final int DEFAULT_TERMINATE_AFTER = 0;
    /** Track the total hit count exactly. */
    public static final int TRACK_TOTAL_HITS_ACCURATE = Integer.MAX_VALUE;
    /** Do not track the total hit count at all. */
    public static final int TRACK_TOTAL_HITS_DISABLED = -1;
    /** Track the total hit count accurately up to this many hits. */
    public static final int DEFAULT_TRACK_TOTAL_HITS_UP_TO = 10000;

    private SearchContext() {
    }
}
