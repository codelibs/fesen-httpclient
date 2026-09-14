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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.terms;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationExecutionException;

import java.io.IOException;
import java.util.Objects;


/**
 * Namespace for the terms-aggregation request types shared by builders and responses.
 *
 * <p>The aggregator itself is node-side and is not carried over; only the bucket-count
 * thresholds a client sends and reads back survive here.</p>
 *
 * @opensearch.internal
 */
public final class TermsAggregator {

    private TermsAggregator() {
    }

    /**
     * Bucket count thresholds
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class BucketCountThresholds implements Writeable, ToXContentFragment {
        private long minDocCount;
        private long shardMinDocCount;
        private int requiredSize;
        private int shardSize;

        /**
         * Creates a new BucketCountThresholds.
         *
         * @param minDocCount the min doc count
         * @param shardMinDocCount the shard min doc count
         * @param requiredSize the required size
         * @param shardSize the shard size
         */
        public BucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            this.minDocCount = minDocCount;
            this.shardMinDocCount = shardMinDocCount;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(requiredSize);
            out.writeInt(shardSize);
            out.writeLong(minDocCount);
            out.writeLong(shardMinDocCount);
        }

        /**
         * Creates a new BucketCountThresholds.
         *
         * @param bucketCountThresholds the bucket count thresholds
         */
        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this(
                bucketCountThresholds.minDocCount,
                bucketCountThresholds.shardMinDocCount,
                bucketCountThresholds.requiredSize,
                bucketCountThresholds.shardSize
            );
        }

        /**
         * Returns the shard min doc count.
         *
         * @return the shard min doc count
         */
        public long getShardMinDocCount() {
            return shardMinDocCount;
        }

        /**
         * Sets the shard min doc count.
         *
         * @param shardMinDocCount the shard min doc count
         */
        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = shardMinDocCount;
        }

        /**
         * Returns the min doc count.
         *
         * @return the min doc count
         */
        public long getMinDocCount() {
            return minDocCount;
        }

        /**
         * Sets the min doc count.
         *
         * @param minDocCount the min doc count
         */
        public void setMinDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        /**
         * Returns the required size.
         *
         * @return the required size
         */
        public int getRequiredSize() {
            return requiredSize;
        }

        /**
         * Sets the required size.
         *
         * @param requiredSize the required size
         */
        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        /**
         * Returns the shard size.
         *
         * @return the shard size
         */
        public int getShardSize() {
            return shardSize;
        }

        /**
         * Sets the shard size.
         *
         * @param shardSize the shard size
         */
        public void setShardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME.getPreferredName(), requiredSize);
            if (shardSize != -1) {
                builder.field(TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME.getPreferredName(), shardSize);
            }
            builder.field(TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), minDocCount);
            builder.field(TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), shardMinDocCount);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, shardSize, minDocCount, shardMinDocCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BucketCountThresholds other = (BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize)
                && Objects.equals(shardSize, other.shardSize)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(shardMinDocCount, other.shardMinDocCount);
        }
    }

    /**
     * BucketCountThresholds type that throws an exception when shardMinDocCount or shardSize are accessed. This is used for
     * deserialization on the coordinator during reduce as shardMinDocCount and shardSize should not be accessed this way on the
     * coordinator.
     *
     * @opensearch.internal
     */
    public static class CoordinatorBucketCountThresholds extends BucketCountThresholds {

        /**
         * Creates a new CoordinatorBucketCountThresholds.
         *
         * @param minDocCount the min doc count
         * @param shardMinDocCount the shard min doc count
         * @param requiredSize the required size
         * @param shardSize the shard size
         */
        public CoordinatorBucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            super(minDocCount, shardMinDocCount, requiredSize, shardSize);
        }

        @Override
        public long getShardMinDocCount() {
            throw new AggregationExecutionException("shard_min_doc_count should not be accessed via CoordinatorBucketCountThresholds");
        }

        @Override
        public int getShardSize() {
            throw new AggregationExecutionException("shard_size should not be accessed via CoordinatorBucketCountThresholds");
        }
    }
}
