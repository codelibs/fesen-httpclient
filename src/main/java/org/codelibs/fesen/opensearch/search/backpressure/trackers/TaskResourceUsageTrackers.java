/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.backpressure.trackers;

import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.tasks.CancellableTask;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.tasks.TaskCancellation;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TaskResourceUsageTrackers is used to hold all the {@link TaskResourceUsageTracker} objects.
 *
 * @opensearch.internal
 */
public class TaskResourceUsageTrackers {
    private final EnumMap<TaskResourceUsageTrackerType, TaskResourceUsageTracker> all;

    /**
     * Creates a new TaskResourceUsageTrackers.
     */
    public TaskResourceUsageTrackers() {
        all = new EnumMap<>(TaskResourceUsageTrackerType.class);
    }

    /**
     * TaskResourceUsageTracker is used to track completions and cancellations of search related tasks.
     * @opensearch.internal
     */
    public static abstract class TaskResourceUsageTracker {
        /**
         * Creates a new TaskResourceUsageTracker.
         */
        public TaskResourceUsageTracker() {
        }

        /**
         * Counts the number of cancellations made due to this tracker.
         */
        private final AtomicLong cancellations = new AtomicLong();
        /**
         * The resource usage breach evaluator.
         */
        protected ResourceUsageBreachEvaluator resourceUsageBreachEvaluator;

        /**
         * Increments the cancellations.
         *
         * @return this instance
         */
        public long incrementCancellations() {
            return cancellations.incrementAndGet();
        }

        /**
         * Returns the cancellations.
         *
         * @return the cancellations
         */
        public long getCancellations() {
            return cancellations.get();
        }

        /**
         * Returns a unique name for this tracker.
         *
         * @return the name
         */
        public abstract String name();

        /**
         * Returns the cancellation reason for the given task, if it's eligible for cancellation.
         *
         * @param task the task
         * @return this instance
         */
        public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
            return resourceUsageBreachEvaluator.evaluate(task);
        }

        /**
         * Returns the tracker's state for tasks as seen in the stats API.
         *
         * @param activeTasks the active tasks
         * @return the stats
         */
        public abstract Stats stats(List<? extends Task> activeTasks);

        private TaskCancellation getTaskCancellation(final CancellableTask task, final List<Runnable> cancellationCallback) {
            Optional<TaskCancellation.Reason> reason = checkAndMaybeGetCancellationReason(task);
            List<TaskCancellation.Reason> reasons = new ArrayList<>();
            reason.ifPresent(reasons::add);

            return new TaskCancellation(task, reasons, cancellationCallback);
        }

        /**
         * Represents the tracker's state as seen in the stats API.
         */
        public interface Stats extends ToXContentObject, Writeable {}

        /**
         * This interface carries the logic to decide whether a task should be cancelled or not
         */
        public interface ResourceUsageBreachEvaluator {
            /**
             * evaluates whether the task is eligible for cancellation based on {@link TaskResourceUsageTracker} implementation
             * @param task is input to this method on which the cancellation evaluation is performed
             * @return a {@link TaskCancellation.Reason} why this task should be cancelled otherwise empty
             */
            public Optional<TaskCancellation.Reason> evaluate(final Task task);
        }
    }
}
