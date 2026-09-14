/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.tasks.resourcetracker;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * Defines the different types of resource stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.1.0")
public enum ResourceStatsType {
    // resource stats of the worker thread reported directly from runnable.
    /**
     * The WORKER_STATS value.
     */
    WORKER_STATS("worker_stats", false);

    private final String statsType;
    private final boolean onlyForAnalysis;

    ResourceStatsType(String statsType, boolean onlyForAnalysis) {
        this.statsType = statsType;
        this.onlyForAnalysis = onlyForAnalysis;
    }

    /**
     * Returns the only for analysis flag.
     *
     * @return the only for analysis flag
     */
    public boolean isOnlyForAnalysis() {
        return onlyForAnalysis;
    }

    @Override
    public String toString() {
        return statsType;
    }
}
