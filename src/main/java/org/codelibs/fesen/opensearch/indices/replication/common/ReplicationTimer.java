/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.replication.common;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A serializable timer that is used to measure the time taken for
 * file replication operations like recovery.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ReplicationTimer implements Writeable {
    private long startTime = 0;
    private long startNanoTime = 0;
    private long time = -1;
    private long stopTime = 0;

    /**
     * Creates a new ReplicationTimer.
     */
    public ReplicationTimer() {}

    /**
     * Creates a new ReplicationTimer by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ReplicationTimer(StreamInput in) throws IOException {
        startTime = in.readVLong();
        startNanoTime = in.readVLong();
        stopTime = in.readVLong();
        time = in.readVLong();
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startTime);
        out.writeVLong(startNanoTime);
        out.writeVLong(stopTime);
        // write a snapshot of current time, which is not per se the time field
        out.writeVLong(time());
    }

    /**
     * Starts this instance.
     */
    public synchronized void start() {
        assert startTime == 0 : "already started";
        startTime = System.currentTimeMillis();
        startNanoTime = System.nanoTime();
    }

    /**
     * Returns start time in millis
     *
     * @return this instance
     */
    public synchronized long startTime() {
        return startTime;
    }

    /**
     * Returns elapsed time in millis, or 0 if timer was not started
     *
     * @return the time
     */
    public synchronized long time() {
        if (startNanoTime == 0) {
            return 0;
        }
        if (time >= 0) {
            return time;
        }
        return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
    }

    /**
     * Returns stop time in millis
     *
     * @return this instance
     */
    public synchronized long stopTime() {
        return stopTime;
    }

    /**
     * Stops this instance.
     */
    public synchronized void stop() {
        assert stopTime == 0 : "already stopped";
        stopTime = Math.max(System.currentTimeMillis(), startTime);
        time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
        assert time >= 0;
    }

    /**
     * Resets this instance.
     */
    public synchronized void reset() {
        startTime = 0;
        startNanoTime = 0;
        time = -1;
        stopTime = 0;
    }
}
