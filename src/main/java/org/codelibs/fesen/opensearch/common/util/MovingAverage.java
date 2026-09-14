/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.util;

/**
 * MovingAverage is used to calculate the moving average of last 'n' observations.
 *
 * @opensearch.internal
 */
public class MovingAverage {
    private final int windowSize;
    private final long[] observations;

    private volatile long count = 0;
    private volatile long sum = 0;
    private volatile double average = 0;

    /**
     * Creates a new MovingAverage.
     *
     * @param windowSize the window size
     */
    public MovingAverage(int windowSize) {
        checkWindowSize(windowSize);
        this.windowSize = windowSize;
        this.observations = new long[windowSize];
    }

    /**
     * Used for changing the window size of {@code MovingAverage}.
     *
     * @param newWindowSize new window size.
     * @return copy of original object with updated size.
     */
    public MovingAverage copyWithSize(int newWindowSize) {
        MovingAverage copy = new MovingAverage(newWindowSize);
        // Start is inclusive, but end is exclusive
        long start, end = count;
        if (isReady() == false) {
            start = 0;
        } else {
            start = end - windowSize;
        }
        // If the newWindow Size is smaller than the elements eligible to be copied over, then we adjust the start value
        if (end - start > newWindowSize) {
            start = end - newWindowSize;
        }
        for (int i = (int) start; i < end; i++) {
            copy.record(observations[i % observations.length]);
        }
        return copy;
    }

    private void checkWindowSize(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("window size must be greater than zero");
        }
    }

    /**
     * Records a new observation and evicts the n-th last observation.
     *
     * @param value the value
     * @return the long
     */
    public synchronized double record(long value) {
        long delta = value - observations[(int) (count % observations.length)];
        observations[(int) (count % observations.length)] = value;

        count++;
        sum += delta;
        average = (double) sum / Math.min(count, observations.length);
        return average;
    }

    /**
     * Returns the average.
     *
     * @return the average
     */
    public double getAverage() {
        return average;
    }

    /**
     * Returns the ready flag.
     *
     * @return the ready flag
     */
    public boolean isReady() {
        return count >= windowSize;
    }
}
