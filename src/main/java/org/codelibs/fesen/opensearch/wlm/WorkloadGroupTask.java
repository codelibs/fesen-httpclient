/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.tasks.CancellableTask;

import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;


/**
 * Base class to define WorkloadGroup tasks
 */
@PublicApi(since = "2.18.0")
public class WorkloadGroupTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupTask.class);
    /**
     * The WORKLOAD_GROUP_ID_HEADER constant.
     */
    public static final String WORKLOAD_GROUP_ID_HEADER = "workloadGroupId";
    /**
     * The DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER constant.
     */
    public static final Supplier<String> DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER = () -> "DEFAULT_WORKLOAD_GROUP";
    private final LongSupplier nanoTimeSupplier;
    private String workloadGroupId;
    private boolean isWorkloadGroupSet = false;

    /**
     * Creates a new WorkloadGroupTask.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param description the description
     * @param parentTaskId the parent task identifier
     * @param headers the headers
     */
    public WorkloadGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, TimeValue.MINUS_ONE, System::nanoTime);
    }

    /**
     * Creates a new WorkloadGroupTask.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param description the description
     * @param parentTaskId the parent task identifier
     * @param headers the headers
     * @param cancelAfterTimeInterval the cancel after time interval
     */
    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        this(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval, System::nanoTime);
    }

    /**
     * Creates a new WorkloadGroupTask.
     *
     * @param id the identifier
     * @param type the type
     * @param action the action
     * @param description the description
     * @param parentTaskId the parent task identifier
     * @param headers the headers
     * @param cancelAfterTimeInterval the cancel after time interval
     * @param nanoTimeSupplier the nano time supplier
     */
    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval,
        LongSupplier nanoTimeSupplier
    ) {
        super(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval);
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * This method should always be called after calling setWorkloadGroupId at least once on this object
     * @return task workloadGroupId
     */
    public final String getWorkloadGroupId() {
        if (workloadGroupId == null) {
            logger.warn("WorkloadGroup _id can't be null, It should be set before accessing it. This is abnormal behaviour ");
        }
        return workloadGroupId;
    }

    /**
     * sets the workloadGroupId from threadContext into the task itself,
     * This method was defined since the workloadGroupId can only be evaluated after task creation
     * @param threadContext current threadContext
     */
    public final void setWorkloadGroupId(final ThreadContext threadContext) {
        isWorkloadGroupSet = true;
        if (threadContext != null && threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER) != null) {
            this.workloadGroupId = threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER);
        } else {
            this.workloadGroupId = DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }
    }

    /**
     * Returns the elapsed time.
     *
     * @return the elapsed time
     */
    public long getElapsedTime() {
        return nanoTimeSupplier.getAsLong() - getStartTimeNanos();
    }

    /**
     * Returns the workload group set flag.
     *
     * @return the workload group set flag
     */
    public boolean isWorkloadGroupSet() {
        return isWorkloadGroupSet;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }
}
