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

package org.codelibs.fesen.opensearch.tasks;

import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.action.NotifyOnceListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ResourceUsageInfo;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.TaskThreadUsage;
import org.codelibs.fesen.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Current task information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Task {
    /**
     * The request header to mark tasks with specific ids
     */
    public static final String X_OPAQUE_ID = "X-Opaque-Id";

    /**
     * This header uniquely identifies a request and can be used by users to track it, for example in logs such as slow logs.
     *
     * Format: 32-character hexadecimal
     *
     * Example:
     *
     * X-Request-Id: 19d538d7c42d09240be001d1e4ff6201
     */
    public static final String X_REQUEST_ID = "X-Request-Id";

    /**
     * The REQUEST_HEADERS constant.
     */
    public static final Set<String> REQUEST_HEADERS = Set.of(Task.X_OPAQUE_ID, Task.X_REQUEST_ID);

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final TaskId parentTask;

    private final Map<String, String> headers;

    private final Map<Long, List<ThreadResourceInfo>> resourceStats;

    private final List<NotifyOnceListener<Task>> resourceTrackingCompletionListeners;

    /**
     * Keeps track of the number of active resource tracking threads for this task. It is initialized to 1 to track
     * the task's own/self thread. When this value becomes 0, all threads have been marked inactive and the resource
     * tracking can be stopped for this task.
     */
    private final AtomicInteger numActiveResourceTrackingThreads = new AtomicInteger(1);

    /**
     * The task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    private final long startTime;

    /**
     * The task's start time as a relative time ({@link System#nanoTime()} style).
     */
    private final long startTimeNanos;

    /**
     * Creates a new Task.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param description the description
     * @param parentTask the parent task
     * @param headers the headers
     */
    public Task(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        this(
            id,
            type,
            action,
            description,
            parentTask,
            System.currentTimeMillis(),
            System.nanoTime(),
            headers,
            new ConcurrentHashMap<>(),
            new ArrayList<>()
        );
    }

    /**
     * Creates a new Task.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param description the description
     * @param parentTask the parent task
     * @param startTime the start time
     * @param startTimeNanos the start time nanoseconds
     * @param headers the headers
     * @param resourceStats the resource stats
     * @param resourceTrackingCompletionListeners the resource tracking completion listeners
     */
    public Task(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        long startTime,
        long startTimeNanos,
        Map<String, String> headers,
        ConcurrentHashMap<Long, List<ThreadResourceInfo>> resourceStats,
        List<NotifyOnceListener<Task>> resourceTrackingCompletionListeners
    ) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentTask = parentTask;
        this.startTime = startTime;
        this.startTimeNanos = startTimeNanos;
        this.headers = headers;
        this.resourceStats = resourceStats;
        this.resourceTrackingCompletionListeners = resourceTrackingCompletionListeners;
    }

    /**
     * Returns task id
     *
     * @return the identifier
     */
    public long getId() {
        return id;
    }

    /**
     * Returns task action
     *
     * @return the action
     */
    public String getAction() {
        return action;
    }

    /**
     * Generates task description
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the task's start time in nanoseconds ({@link System#nanoTime()} style).
     *
     * @return the start time nanoseconds
     */
    public long getStartTimeNanos() {
        return startTimeNanos;
    }

    /**
     * Returns id of the parent task or NO_PARENT_ID if the task doesn't have any parent tasks
     *
     * @return the parent task identifier
     */
    public TaskId getParentTaskId() {
        return parentTask;
    }

    /**
     * Build a status for this task or null if this task doesn't have status.
     * Since most tasks don't have status this defaults to returning null. While
     * this can never perform IO it might be a costly operation, requiring
     * collating lists of results, etc. So only use it if you need the value.
     *
     * @return the status
     */
    public Status getStatus() {
        return null;
    }

    /**
     * Returns current total resource usage of the task.
     * Currently, this method is only called on demand, during get and listing of tasks.
     * In the future, these values can be cached as an optimization.
     *
     * @return the total resource stats
     */
    public TaskResourceUsage getTotalResourceStats() {
        return new TaskResourceUsage(getTotalResourceUtilization(ResourceStats.CPU), getTotalResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current average per-execution resource usage of the task.
     *
     * @return the average resource stats
     */
    public TaskResourceUsage getAverageResourceStats() {
        return new TaskResourceUsage(getAverageResourceUtilization(ResourceStats.CPU), getAverageResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current min per-execution resource usage of the task.
     *
     * @return the min resource stats
     */
    public TaskResourceUsage getMinResourceStats() {
        return new TaskResourceUsage(getMinResourceUtilization(ResourceStats.CPU), getMinResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current max per-execution resource usage of the task.
     *
     * @return the max resource stats
     */
    public TaskResourceUsage getMaxResourceStats() {
        return new TaskResourceUsage(getMaxResourceUtilization(ResourceStats.CPU), getMaxResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns total resource consumption for a specific task stat.
     *
     * @param stats the stats
     * @return the total resource utilization
     */
    public long getTotalResourceUtilization(ResourceStats stats) {
        long totalResourceConsumption = 0L;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    totalResourceConsumption += statsInfo.getTotalValue();
                }
            }
        }
        return totalResourceConsumption;
    }

    /**
     * Returns average per-execution resource consumption for a specific task stat.
     */
    private long getAverageResourceUtilization(ResourceStats stats) {
        long totalResourceConsumption = 0L;
        int threadResourceInfoCount = 0;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    totalResourceConsumption += statsInfo.getTotalValue();
                    threadResourceInfoCount++;
                }
            }
        }
        return (threadResourceInfoCount > 0) ? totalResourceConsumption / threadResourceInfoCount : 0;
    }

    /**
     * Returns minimum per-execution resource consumption for a specific task stat.
     */
    private long getMinResourceUtilization(ResourceStats stats) {
        if (resourceStats.size() == 0) {
            return 0L;
        }
        long minResourceConsumption = Long.MAX_VALUE;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    minResourceConsumption = Math.min(minResourceConsumption, statsInfo.getTotalValue());
                }
            }
        }
        return minResourceConsumption;
    }

    /**
     * Returns maximum per-execution resource consumption for a specific task stat.
     */
    private long getMaxResourceUtilization(ResourceStats stats) {
        long maxResourceConsumption = 0L;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    maxResourceConsumption = Math.max(maxResourceConsumption, statsInfo.getTotalValue());
                }
            }
        }
        return maxResourceConsumption;
    }

    /**
     * Report of the internal status of a task. These can vary wildly from task
     * to task because each task is implemented differently but we should try
     * to keep each task consistent from version to version where possible.
     * That means each implementation of {@linkplain Task.Status#toXContent}
     * should avoid making backwards incompatible changes to the rendered
     * result. But if we change the way a request is implemented it might not
     * be possible to preserve backwards compatibility. In that case, we
     * <b>can</b> change this on version upgrade but we should be careful
     * because some statuses (reindex) have become defacto standardized because
     * they are used by systems like Kibana.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface Status extends ToXContentObject, NamedWriteable {}
}
