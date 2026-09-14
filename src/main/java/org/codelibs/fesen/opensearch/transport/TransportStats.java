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

package org.codelibs.fesen.opensearch.transport;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for transport activity
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TransportStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOutboundConnections;
    private final long rxCount;
    private final long rxSize;
    private final long txCount;
    private final long txSize;

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new TransportStats object.
     * @param builder The builder instance containing all the values.
     */
    private TransportStats(Builder builder) {
        this.serverOpen = builder.serverOpen;
        this.totalOutboundConnections = builder.totalOutboundConnections;
        this.rxCount = builder.rxCount;
        this.rxSize = builder.rxSize;
        this.txCount = builder.txCount;
        this.txSize = builder.txSize;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     *
     * @param serverOpen the server open
     * @param totalOutboundConnections the total outbound connections
     * @param rxCount the rx count
     * @param rxSize the rx size
     * @param txCount the tx count
     * @param txSize the tx size
     */
    @Deprecated
    public TransportStats(long serverOpen, long totalOutboundConnections, long rxCount, long rxSize, long txCount, long txSize) {
        this.serverOpen = serverOpen;
        this.totalOutboundConnections = totalOutboundConnections;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
    }

    /**
     * Creates a new TransportStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOutboundConnections = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOutboundConnections);
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
    }

    /**
     * Builder for the {@link TransportStats} class.
     * Provides a fluent API for constructing a TransportStats object.
     */
    public static class Builder {
        private long serverOpen = 0;
        private long totalOutboundConnections = 0;
        private long rxCount = 0;
        private long rxSize = 0;
        private long txCount = 0;
        private long txSize = 0;

        /**
         * Creates a new Builder.
         */
        public Builder() {}

        /**
         * Returns the server open.
         *
         * @param serverOpen the server open
         * @return the server open
         */
        public Builder serverOpen(long serverOpen) {
            this.serverOpen = serverOpen;
            return this;
        }

        /**
         * Returns the total outbound connections.
         *
         * @param connections the connections
         * @return the total outbound connections
         */
        public Builder totalOutboundConnections(long connections) {
            this.totalOutboundConnections = connections;
            return this;
        }

        /**
         * Returns the rx count.
         *
         * @param count the count
         * @return the rx count
         */
        public Builder rxCount(long count) {
            this.rxCount = count;
            return this;
        }

        /**
         * Returns the rx size.
         *
         * @param size the size
         * @return the rx size
         */
        public Builder rxSize(long size) {
            this.rxSize = size;
            return this;
        }

        /**
         * Returns the tx count.
         *
         * @param count the count
         * @return the tx count
         */
        public Builder txCount(long count) {
            this.txCount = count;
            return this;
        }

        /**
         * Returns the tx size.
         *
         * @param size the size
         * @return the tx size
         */
        public Builder txSize(long size) {
            this.txSize = size;
            return this;
        }

        /**
         * Creates a {@link TransportStats} object from the builder's current state.
         * @return A new TransportStats instance.
         */
        public TransportStats build() {
            return new TransportStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.SERVER_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OUTBOUND_CONNECTIONS, totalOutboundConnections);
        builder.field(Fields.RX_COUNT, rxCount);
        builder.humanReadableField(Fields.RX_SIZE_IN_BYTES, Fields.RX_SIZE, new ByteSizeValue(rxSize));
        builder.field(Fields.TX_COUNT, txCount);
        builder.humanReadableField(Fields.TX_SIZE_IN_BYTES, Fields.TX_SIZE, new ByteSizeValue(txSize));
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String SERVER_OPEN = "server_open";
        static final String TOTAL_OUTBOUND_CONNECTIONS = "total_outbound_connections";
        static final String RX_COUNT = "rx_count";
        static final String RX_SIZE = "rx_size";
        static final String RX_SIZE_IN_BYTES = "rx_size_in_bytes";
        static final String TX_COUNT = "tx_count";
        static final String TX_SIZE = "tx_size";
        static final String TX_SIZE_IN_BYTES = "tx_size_in_bytes";
    }
}
