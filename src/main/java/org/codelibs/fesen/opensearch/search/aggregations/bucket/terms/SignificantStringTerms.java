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

import org.apache.lucene.util.BytesRef;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.DocValueFormat;
import org.codelibs.fesen.opensearch.search.aggregations.InternalAggregations;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the running the significant terms aggregation on a String field.
 *
 * @opensearch.internal
 */
public class SignificantStringTerms extends InternalMappedSignificantTerms<SignificantStringTerms, SignificantStringTerms.Bucket> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "sigsterms";

    /**
     * Bucket for significant string values
     *
     * @opensearch.internal
     */
    public static class Bucket extends InternalSignificantTerms.Bucket<Bucket> {

        BytesRef termBytes;

        /**
         * Creates a new Bucket.
         *
         * @param term the term
         * @param subsetDf the subset df
         * @param subsetSize the subset size
         * @param supersetDf the superset df
         * @param supersetSize the superset size
         * @param aggregations the aggregations
         * @param format the format
         * @param score the score
         */
        public Bucket(
            BytesRef term,
            long subsetDf,
            long subsetSize,
            long supersetDf,
            long supersetSize,
            InternalAggregations aggregations,
            DocValueFormat format,
            double score
        ) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format);
            this.termBytes = term;
            this.score = score;
        }

        /**
         * Read from a stream.
         *
         * @param in the input to read from
         * @param subsetSize the subset size
         * @param supersetSize the superset size
         * @param format the format
         * @throws IOException if an I/O error occurs
         */
        public Bucket(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) throws IOException {
            super(in, subsetSize, supersetSize, format);
            termBytes = in.readBytesRef();
            subsetDf = in.readVLong();
            supersetDf = in.readVLong();
            score = in.readDouble();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
            out.writeVLong(subsetDf);
            out.writeVLong(supersetDf);
            out.writeDouble(getSignificanceScore());
            aggregations.writeTo(out);
        }

        @Override
        public Number getKeyAsNumber() {
            // this method is needed for scripted numeric aggregations
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes).toString();
        }

        @Override
        public String getKey() {
            return getKeyAsString();
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;

            return super.equals(obj) && Objects.equals(termBytes, ((SignificantStringTerms.Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    /**
     * Creates a new SignificantStringTerms.
     *
     * @param name the name
     * @param metadata the metadata
     * @param format the format
     * @param subsetSize the subset size
     * @param supersetSize the superset size
     * @param significanceHeuristic the significance heuristic
     * @param buckets the buckets
     * @param bucketCountThresholds the bucket count thresholds
     */
    public SignificantStringTerms(
        String name,
        Map<String, Object> metadata,
        DocValueFormat format,
        long subsetSize,
        long supersetSize,
        SignificanceHeuristic significanceHeuristic,
        List<Bucket> buckets,
        TermsAggregator.BucketCountThresholds bucketCountThresholds
    ) {
        super(name, metadata, format, subsetSize, supersetSize, significanceHeuristic, buckets, bucketCountThresholds);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SignificantStringTerms create(List<SignificantStringTerms.Bucket> buckets) {
        return new SignificantStringTerms(
            name,
            metadata,
            format,
            subsetSize,
            supersetSize,
            significanceHeuristic,
            buckets,
            bucketCountThresholds
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, SignificantStringTerms.Bucket prototype) {
        return new Bucket(
            prototype.termBytes,
            prototype.subsetDf,
            prototype.subsetSize,
            prototype.supersetDf,
            prototype.supersetSize,
            aggregations,
            prototype.format,
            prototype.score
        );
    }

    @Override
    protected SignificantStringTerms create(long subsetSize, long supersetSize, List<Bucket> buckets) {
        return new SignificantStringTerms(
            getName(),
            getMetadata(),
            format,
            subsetSize,
            supersetSize,
            significanceHeuristic,
            buckets,
            bucketCountThresholds
        );
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    Bucket createBucket(
        long subsetDf,
        long subsetSize,
        long supersetDf,
        long supersetSize,
        InternalAggregations aggregations,
        SignificantStringTerms.Bucket prototype
    ) {
        return new Bucket(prototype.termBytes, subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format, prototype.score);
    }
}
