/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.backpressure.trackers;

/**
 * Defines the type of TaskResourceUsageTracker.
 */
public enum TaskResourceUsageTrackerType {
    /**
     * The CPU_USAGE_TRACKER value.
     */
    CPU_USAGE_TRACKER("cpu_usage_tracker"),
    /**
     * The HEAP_USAGE_TRACKER value.
     */
    HEAP_USAGE_TRACKER("heap_usage_tracker"),
    /**
     * The ELAPSED_TIME_TRACKER value.
     */
    ELAPSED_TIME_TRACKER("elapsed_time_tracker"),
    /**
     * The NATIVE_MEMORY_USAGE_TRACKER value.
     */
    NATIVE_MEMORY_USAGE_TRACKER("native_memory_usage_tracker");

    private final String name;

    TaskResourceUsageTrackerType(String name) {
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
