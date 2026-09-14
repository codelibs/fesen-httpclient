/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * Enum for different Search Phases in OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public enum SearchPhaseName {
    /**
     * The DFS_PRE_QUERY value.
     */
    DFS_PRE_QUERY("dfs_pre_query"),
    /**
     * The QUERY value.
     */
    QUERY("query"),
    /**
     * The FETCH value.
     */
    FETCH("fetch"),
    /**
     * The DFS_QUERY value.
     */
    DFS_QUERY("dfs_query"),
    /**
     * The EXPAND value.
     */
    EXPAND("expand"),
    /**
     * The CAN_MATCH value.
     */
    CAN_MATCH("can_match");

    private final String name;

    SearchPhaseName(final String name) {
        this.name = name;
    }

    /**
     * Returns the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }
}
