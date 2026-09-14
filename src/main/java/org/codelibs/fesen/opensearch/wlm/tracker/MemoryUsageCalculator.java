/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.wlm.tracker;

import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.codelibs.fesen.opensearch.monitor.jvm.JvmStats;
import org.codelibs.fesen.opensearch.wlm.WorkloadGroupTask;

import java.util.List;

/**
 * class to help make memory usage calculations for the workload group
 */
public class MemoryUsageCalculator extends ResourceUsageCalculator {
    /**
     * The HEAP_SIZE_BYTES constant.
     */
    public static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();
    /**
     * The INSTANCE constant.
     */
    public static final MemoryUsageCalculator INSTANCE = new MemoryUsageCalculator();

    private MemoryUsageCalculator() {}

    @Override
    public double calculateResourceUsage(List<WorkloadGroupTask> tasks) {
        return tasks.stream().mapToDouble(this::calculateTaskResourceUsage).sum();
    }

    @Override
    public double calculateTaskResourceUsage(WorkloadGroupTask task) {
        return (1.0f * task.getTotalResourceUtilization(ResourceStats.MEMORY)) / HEAP_SIZE_BYTES;
    }
}
