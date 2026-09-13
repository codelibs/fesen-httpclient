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

package org.codelibs.fesen.opensearch.action.admin.indices.datastream;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastResponse;
import org.codelibs.fesen.opensearch.cluster.routing.ShardRouting;
import org.codelibs.fesen.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.store.StoreStats;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Transport action for retrieving datastream stats
 *
 * @opensearch.internal
 */
public class DataStreamsStatsAction extends ActionType<DataStreamsStatsAction.Response> {

    public static final DataStreamsStatsAction INSTANCE = new DataStreamsStatsAction();
    public static final String NAME = "indices:monitor/data_stream/stats";

    public DataStreamsStatsAction() {
        super(NAME, DataStreamsStatsAction.Response::new);
    }

    /**
     * Request for Data Streams Stats
     *
     * @opensearch.internal
     */
    public static class Request extends BroadcastRequest<Request> {
        public Request() {
            super((String[]) null);
        }
    }

    /**
     * Response for Data Streams Stats
     *
     * @opensearch.internal
     */
    public static class Response extends BroadcastResponse {
        private final int dataStreamCount;
        private final int backingIndices;
        private final ByteSizeValue totalStoreSize;
        private final DataStreamStats[] dataStreams;

        public Response(
            int totalShards,
            int successfulShards,
            int failedShards,
            List<DefaultShardOperationFailedException> shardFailures,
            int dataStreamCount,
            int backingIndices,
            ByteSizeValue totalStoreSize,
            DataStreamStats[] dataStreams
        ) {
            super(totalShards, successfulShards, failedShards, shardFailures);
            this.dataStreamCount = dataStreamCount;
            this.backingIndices = backingIndices;
            this.totalStoreSize = totalStoreSize;
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.dataStreamCount = in.readVInt();
            this.backingIndices = in.readVInt();
            this.totalStoreSize = new ByteSizeValue(in);
            this.dataStreams = in.readArray(DataStreamStats::new, DataStreamStats[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(dataStreamCount);
            out.writeVInt(backingIndices);
            totalStoreSize.writeTo(out);
            out.writeArray(dataStreams);
        }

        @Override
        protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("data_stream_count", dataStreamCount);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("total_store_size_bytes", "total_store_size", totalStoreSize);
            builder.array("data_streams", (Object[]) dataStreams);
        }

        public int getDataStreamCount() {
            return dataStreamCount;
        }

        public int getBackingIndices() {
            return backingIndices;
        }

        public ByteSizeValue getTotalStoreSize() {
            return totalStoreSize;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response response = (Response) obj;
            return dataStreamCount == response.dataStreamCount
                && backingIndices == response.backingIndices
                && Objects.equals(totalStoreSize, response.totalStoreSize)
                && Arrays.equals(dataStreams, response.dataStreams);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataStreamCount, backingIndices, totalStoreSize);
            result = 31 * result + Arrays.hashCode(dataStreams);
            return result;
        }

        @Override
        public String toString() {
            return "Response{"
                + "dataStreamCount="
                + dataStreamCount
                + ", backingIndices="
                + backingIndices
                + ", totalStoreSize="
                + totalStoreSize
                + ", dataStreams="
                + Arrays.toString(dataStreams)
                + '}';
        }
    }

    /**
     * The Data Streams Stats container
     *
     * @opensearch.internal
     */
    public static class DataStreamStats implements ToXContentObject, Writeable {
        private final String dataStream;
        private final int backingIndices;
        private final ByteSizeValue storeSize;
        private final long maximumTimestamp;

        public DataStreamStats(String dataStream, int backingIndices, ByteSizeValue storeSize, long maximumTimestamp) {
            this.dataStream = dataStream;
            this.backingIndices = backingIndices;
            this.storeSize = storeSize;
            this.maximumTimestamp = maximumTimestamp;
        }

        public DataStreamStats(StreamInput in) throws IOException {
            this.dataStream = in.readString();
            this.backingIndices = in.readVInt();
            this.storeSize = new ByteSizeValue(in);
            this.maximumTimestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStream);
            out.writeVInt(backingIndices);
            storeSize.writeTo(out);
            out.writeVLong(maximumTimestamp);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("data_stream", dataStream);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("store_size_bytes", "store_size", storeSize);
            builder.field("maximum_timestamp", maximumTimestamp);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DataStreamStats that = (DataStreamStats) obj;
            return backingIndices == that.backingIndices
                && maximumTimestamp == that.maximumTimestamp
                && Objects.equals(dataStream, that.dataStream)
                && Objects.equals(storeSize, that.storeSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStream, backingIndices, storeSize, maximumTimestamp);
        }

        @Override
        public String toString() {
            return "DataStreamStats{"
                + "dataStream='"
                + dataStream
                + '\''
                + ", backingIndices="
                + backingIndices
                + ", storeSize="
                + storeSize
                + ", maximumTimestamp="
                + maximumTimestamp
                + '}';
        }
    }

}
