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

package org.codelibs.fesen.opensearch.cluster.service;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.Priority;
import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.text.Text;

import java.io.IOException;

/**
 * Represents a task that is pending in the cluster
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PendingClusterTask implements Writeable {

    private long insertOrder;
    private Priority priority;
    private Text source;
    private long timeInQueue;
    private boolean executing;
    private long timeInExecution;

    /**
     * Creates a new PendingClusterTask by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    @InternalApi
    public PendingClusterTask(StreamInput in) throws IOException {
        insertOrder = in.readVLong();
        priority = Priority.readFrom(in);
        source = in.readText();
        timeInQueue = in.readLong();
        executing = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_3_1_0)) {
            timeInExecution = in.readLong();
        }
    }

    /**
     * Creates a new PendingClusterTask.
     *
     * @param insertOrder the insert order
     * @param priority the priority
     * @param source the source
     * @param timeInQueue the time in queue
     * @param executing the executing
     * @param timeInExecution the time in execution
     */
    @InternalApi
    public PendingClusterTask(long insertOrder, Priority priority, Text source, long timeInQueue, boolean executing, long timeInExecution) {
        assert timeInQueue >= 0 : "got a negative timeInQueue [" + timeInQueue + "]";
        assert insertOrder >= 0 : "got a negative insertOrder [" + insertOrder + "]";
        assert timeInExecution >= 0 : "got a negative timeInExecution [" + timeInExecution + "]";
        this.insertOrder = insertOrder;
        this.priority = priority;
        this.source = source;
        this.timeInQueue = timeInQueue;
        this.executing = executing;
        this.timeInExecution = timeInExecution;
    }

    /**
     * Returns the insert order.
     *
     * @return the insert order
     */
    public long getInsertOrder() {
        return insertOrder;
    }

    /**
     * Returns the priority.
     *
     * @return the priority
     */
    public Priority getPriority() {
        return priority;
    }

    /**
     * Returns the source.
     *
     * @return the source
     */
    public Text getSource() {
        return source;
    }

    /**
     * Returns the time in queue in milliseconds.
     *
     * @return the time in queue in milliseconds
     */
    public long getTimeInQueueInMillis() {
        return timeInQueue;
    }

    /**
     * Returns the time in execution in milliseconds.
     *
     * @return the time in execution in milliseconds
     */
    public long getTimeInExecutionInMillis() {
        return timeInExecution;
    }

    /**
     * Returns the time in queue.
     *
     * @return the time in queue
     */
    public TimeValue getTimeInQueue() {
        return new TimeValue(getTimeInQueueInMillis());
    }

    /**
     * Returns the time in execution.
     *
     * @return the time in execution
     */
    public TimeValue getTimeInExecution() {
        return new TimeValue(getTimeInExecutionInMillis());
    }

    /**
     * Returns the executing flag.
     *
     * @return the executing flag
     */
    public boolean isExecuting() {
        return executing;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(insertOrder);
        Priority.writeTo(priority, out);
        out.writeText(source);
        out.writeLong(timeInQueue);
        out.writeBoolean(executing);
        if (out.getVersion().onOrAfter(Version.V_3_1_0)) {
            out.writeLong(timeInExecution);
        }
    }
}
