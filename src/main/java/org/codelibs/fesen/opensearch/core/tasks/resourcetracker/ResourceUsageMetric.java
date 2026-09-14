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
 * Information about resource usage
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class ResourceUsageMetric {
    private final ResourceStats stats;
    private final long value;

    /**
     * Creates a new ResourceUsageMetric.
     *
     * @param stats the stats
     * @param value the value
     */
    public ResourceUsageMetric(ResourceStats stats, long value) {
        this.stats = stats;
        this.value = value;
    }

    /**
     * Returns the stats.
     *
     * @return the stats
     */
    public ResourceStats getStats() {
        return stats;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public long getValue() {
        return value;
    }
}
