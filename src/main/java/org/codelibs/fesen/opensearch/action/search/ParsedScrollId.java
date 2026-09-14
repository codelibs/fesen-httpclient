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

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.util.Arrays;

/**
 * Search scroll id that has been parsed
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ParsedScrollId {

    /**
     * The QUERY_THEN_FETCH_TYPE constant.
     */
    public static final String QUERY_THEN_FETCH_TYPE = "queryThenFetch";

    /**
     * The QUERY_AND_FETCH_TYPE constant.
     */
    public static final String QUERY_AND_FETCH_TYPE = "queryAndFetch";

    private final String source;

    private final String type;

    private final SearchContextIdForNode[] context;
    private final String[] originalIndices;

    ParsedScrollId(String source, String type, SearchContextIdForNode[] context, String[] originalIndices) {
        this.source = source;
        this.type = type;
        this.context = context;
        this.originalIndices = originalIndices;
    }

    /**
     * Returns the source.
     *
     * @return the source
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the context.
     *
     * @return the context
     */
    public SearchContextIdForNode[] getContext() {
        return context;
    }

    /**
     * Returns the original indices.
     *
     * @return the original indices
     */
    public String[] getOriginalIndices() {
        return originalIndices;
    }

    /**
     * Returns the local indices flag.
     *
     * @return the local indices flag
     */
    public boolean hasLocalIndices() {
        return Arrays.stream(context).anyMatch(c -> c.getClusterAlias() == null);
    }
}
