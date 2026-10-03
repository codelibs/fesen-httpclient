/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-node snapshot of all configured adaptive concurrency limiters,
 * exposed via the {@code /_nodes/stats} API under the {@code concurrency_limiters} key.
 */
public class ActionConcurrencyLimiterStats implements Writeable, ToXContentFragment {

    /**
     * Snapshot for a single action alias.
     */
    public static class ActionLimiterSnapshot implements Writeable, ToXContentFragment {

        private static final long RTT_UNAVAILABLE = -1L;

        private final String alias;
        private final String actionName;
        private final String mode;
        private final String algorithm;
        private final int currentLimit;
        private final int inFlight;
        private final long totalRejected;
        private final long lastRttMillis;
        private final long rttNoLoadMillis;

        /**
         * Creates a snapshot of a single action limiter.
         *
         * @param alias the action alias
         * @param actionName the action name
         * @param mode the limiter mode
         * @param algorithm the limit algorithm
         * @param currentLimit the current concurrency limit
         * @param inFlight the number of in-flight requests
         * @param totalRejected the total number of rejected requests
         * @param lastRttMillis the last round-trip time in milliseconds, or {@code -1} if unavailable
         * @param rttNoLoadMillis the no-load round-trip time in milliseconds, or {@code -1} if unavailable
         */
        public ActionLimiterSnapshot(
            String alias,
            String actionName,
            String mode,
            String algorithm,
            int currentLimit,
            int inFlight,
            long totalRejected,
            long lastRttMillis,
            long rttNoLoadMillis
        ) {
            this.alias = alias;
            this.actionName = actionName;
            this.mode = mode;
            this.algorithm = algorithm;
            this.currentLimit = currentLimit;
            this.inFlight = inFlight;
            this.totalRejected = totalRejected;
            this.lastRttMillis = lastRttMillis;
            this.rttNoLoadMillis = rttNoLoadMillis;
        }

        /**
         * Creates a snapshot by reading it from the given input.
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        public ActionLimiterSnapshot(StreamInput in) throws IOException {
            this.alias = in.readString();
            this.actionName = in.readString();
            this.mode = in.readString();
            this.algorithm = in.readString();
            this.currentLimit = in.readVInt();
            this.inFlight = in.readVInt();
            this.totalRejected = in.readVLong();
            this.lastRttMillis = in.readLong();
            this.rttNoLoadMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(alias);
            out.writeString(actionName);
            out.writeString(mode);
            out.writeString(algorithm);
            out.writeVInt(currentLimit);
            out.writeVInt(inFlight);
            out.writeVLong(totalRejected);
            out.writeLong(lastRttMillis);
            out.writeLong(rttNoLoadMillis);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(alias);
            builder.field("action_name", actionName);
            builder.field("mode", mode);
            builder.field("algorithm", algorithm);
            builder.field("current_limit", currentLimit);
            builder.field("in_flight", inFlight);
            builder.field("total_rejected", totalRejected);
            if (lastRttMillis != RTT_UNAVAILABLE) builder.field("last_rtt_millis", lastRttMillis);
            if (rttNoLoadMillis != RTT_UNAVAILABLE) builder.field("rtt_no_load_millis", rttNoLoadMillis);
            builder.endObject();
            return builder;
        }

        /**
         * Returns the action alias.
         *
         * @return the action alias
         */
        public String getAlias() {
            return alias;
        }

        /**
         * Returns the action name.
         *
         * @return the action name
         */
        public String getActionName() {
            return actionName;
        }

        /**
         * Returns the limiter mode.
         *
         * @return the limiter mode
         */
        public String getMode() {
            return mode;
        }

        /**
         * Returns the limit algorithm.
         *
         * @return the limit algorithm
         */
        public String getAlgorithm() {
            return algorithm;
        }

        /**
         * Returns the current concurrency limit.
         *
         * @return the current concurrency limit
         */
        public int getCurrentLimit() {
            return currentLimit;
        }

        /**
         * Returns the number of in-flight requests.
         *
         * @return the number of in-flight requests
         */
        public int getInFlight() {
            return inFlight;
        }

        /**
         * Returns the total number of rejected requests.
         *
         * @return the total number of rejected requests
         */
        public long getTotalRejected() {
            return totalRejected;
        }

        /**
         * Returns the last round-trip time in milliseconds.
         *
         * @return the last round-trip time in milliseconds
         */
        public long getLastRttMillis() {
            return lastRttMillis;
        }

        /**
         * Returns the no-load round-trip time in milliseconds.
         *
         * @return the no-load round-trip time in milliseconds
         */
        public long getRttNoLoadMillis() {
            return rttNoLoadMillis;
        }
    }

    private final List<ActionLimiterSnapshot> snapshots;

    /**
     * Creates the stats from the given limiter snapshots.
     *
     * @param snapshots the snapshots, one per configured action alias
     */
    public ActionConcurrencyLimiterStats(List<ActionLimiterSnapshot> snapshots) {
        this.snapshots = Collections.unmodifiableList(snapshots);
    }

    /**
     * Creates the stats by reading them from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ActionConcurrencyLimiterStats(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<ActionLimiterSnapshot> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new ActionLimiterSnapshot(in));
        }
        this.snapshots = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(snapshots.size());
        for (ActionLimiterSnapshot snap : snapshots) {
            snap.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("concurrency_limiters");
        for (ActionLimiterSnapshot snap : snapshots) {
            snap.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns the limiter snapshots.
     *
     * @return the snapshots, one per configured action alias
     */
    public List<ActionLimiterSnapshot> getSnapshots() {
        return snapshots;
    }
}
