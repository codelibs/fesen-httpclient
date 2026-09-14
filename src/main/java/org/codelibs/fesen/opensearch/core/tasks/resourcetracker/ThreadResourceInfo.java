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
 * Resource consumption information about a particular execution of thread.
 * <p>
 * It captures the resource usage information about a particular execution of thread
 * for a specific stats type like worker_stats or response_stats etc.,
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class ThreadResourceInfo {
    private final long threadId;
    private volatile boolean isActive = true;
    private final ResourceStatsType statsType;
    private final ResourceUsageInfo resourceUsageInfo;

    /**
     * Creates a new ThreadResourceInfo.
     *
     * @param threadId the thread identifier
     * @param statsType the stats type
     * @param resourceUsageMetrics the resource usage metrics
     */
    public ThreadResourceInfo(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        this.threadId = threadId;
        this.statsType = statsType;
        this.resourceUsageInfo = new ResourceUsageInfo(resourceUsageMetrics);
    }

    /**
     * Updates thread's resource consumption information.
     *
     * @param resourceUsageMetrics the resource usage metrics
     */
    public void recordResourceUsageMetrics(ResourceUsageMetric... resourceUsageMetrics) {
        resourceUsageInfo.recordResourceUsageMetrics(resourceUsageMetrics);
    }

    /**
     * Sets the active.
     *
     * @param isActive the is active
     */
    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }

    /**
     * Returns the active flag.
     *
     * @return the active flag
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * Returns the stats type.
     *
     * @return the stats type
     */
    public ResourceStatsType getStatsType() {
        return statsType;
    }

    /**
     * Returns the thread identifier.
     *
     * @return the thread identifier
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * Returns the resource usage info.
     *
     * @return the resource usage info
     */
    public ResourceUsageInfo getResourceUsageInfo() {
        return resourceUsageInfo;
    }

    @Override
    public String toString() {
        return resourceUsageInfo + ", stats_type=" + statsType + ", is_active=" + isActive + ", threadId=" + threadId;
    }
}
