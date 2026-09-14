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

package org.codelibs.fesen.opensearch.index.flush;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Encapsulates statistics for flush
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FlushStats implements Writeable, ToXContentFragment {

    private long total;
    private long periodic;
    private long totalTimeInMillis;

    /**
     * Creates a new FlushStats.
     */
    public FlushStats() {

    }

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new FlushStats object.
     * @param builder The builder instance containing all the values.
     */
    private FlushStats(Builder builder) {
        this.total = builder.total;
        this.periodic = builder.periodic;
        this.totalTimeInMillis = builder.totalTimeInMillis;
    }

    /**
     * Creates a new FlushStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public FlushStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        periodic = in.readVLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     *
     * @param total the total
     * @param periodic the periodic
     * @param totalTimeInMillis the total time in milliseconds
     */
    @Deprecated
    public FlushStats(long total, long periodic, long totalTimeInMillis) {
        this.total = total;
        this.periodic = periodic;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    /**
     * Adds this instance.
     *
     * @param flushStats the flush stats
     */
    public void add(FlushStats flushStats) {
        addTotals(flushStats);
    }

    /**
     * Adds the totals.
     *
     * @param flushStats the flush stats
     */
    public void addTotals(FlushStats flushStats) {
        if (flushStats == null) {
            return;
        }
        this.total += flushStats.total;
        this.periodic += flushStats.periodic;
        this.totalTimeInMillis += flushStats.totalTimeInMillis;
    }

    /**
     * The total time merges have been executed.
     *
     * @return the total time
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * Builder for the {@link FlushStats} class.
     * Provides a fluent API for constructing a FlushStats object.
     */
    public static class Builder {
        private long total = 0;
        private long periodic = 0;
        private long totalTimeInMillis = 0;

        /**
         * Creates a new Builder.
         */
        public Builder() {}

        /**
         * Returns the total.
         *
         * @param total the total
         * @return the total
         */
        public Builder total(long total) {
            this.total = total;
            return this;
        }

        /**
         * Returns the periodic.
         *
         * @param periodic the periodic
         * @return the periodic
         */
        public Builder periodic(long periodic) {
            this.periodic = periodic;
            return this;
        }

        /**
         * Returns the total time in milliseconds.
         *
         * @param time the time
         * @return the total time in milliseconds
         */
        public Builder totalTimeInMillis(long time) {
            this.totalTimeInMillis = time;
            return this;
        }

        /**
         * Creates a {@link FlushStats} object from the builder's current state.
         *
         * @return A new FlushStats instance.
         */
        public FlushStats build() {
            return new FlushStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FLUSH);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PERIODIC, periodic);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for flush statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String FLUSH = "flush";
        static final String TOTAL = "total";
        static final String PERIODIC = "periodic";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(periodic);
    }
}
