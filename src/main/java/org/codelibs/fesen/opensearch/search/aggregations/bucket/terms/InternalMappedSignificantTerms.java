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

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.DocValueFormat;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of mapped significant terms
 *
 * @param <A> the aggregation type
 * @param <B> the builder type
 * @opensearch.internal
 */
public abstract class InternalMappedSignificantTerms<
    A extends InternalMappedSignificantTerms<A, B>,
    B extends InternalSignificantTerms.Bucket<B>> extends InternalSignificantTerms<A, B> {

    /**
     * The format.
     */
    protected final DocValueFormat format;
    /**
     * The subset size.
     */
    protected final long subsetSize;
    /**
     * The superset size.
     */
    protected final long supersetSize;
    /**
     * The significance heuristic.
     */
    protected final SignificanceHeuristic significanceHeuristic;
    /**
     * The buckets.
     */
    protected final List<B> buckets;
    /**
     * The bucket map.
     */
    protected Map<String, B> bucketMap;

    /**
     * Creates a new InternalMappedSignificantTerms.
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
    protected InternalMappedSignificantTerms(
        String name,
        Map<String, Object> metadata,
        DocValueFormat format,
        long subsetSize,
        long supersetSize,
        SignificanceHeuristic significanceHeuristic,
        List<B> buckets,
        TermsAggregator.BucketCountThresholds bucketCountThresholds
    ) {
        super(name, bucketCountThresholds, metadata);
        this.format = format;
        this.buckets = buckets;
        this.subsetSize = subsetSize;
        this.supersetSize = supersetSize;
        this.significanceHeuristic = significanceHeuristic;
    }

    /**
     * Creates a new InternalMappedSignificantTerms by reading it from the given input.
     *
     * @param in the input to read from
     * @param bucketReader the bucket reader
     * @throws IOException if an I/O error occurs
     */
    protected InternalMappedSignificantTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        subsetSize = in.readVLong();
        supersetSize = in.readVLong();
        significanceHeuristic = in.readNamedWriteable(SignificanceHeuristic.class);
        buckets = in.readList(stream -> bucketReader.read(stream, subsetSize, supersetSize, format));
    }

    @Override
    protected final void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVLong(subsetSize);
        out.writeVLong(supersetSize);
        out.writeNamedWriteable(significanceHeuristic);
        out.writeList(buckets);
    }

    @Override
    public Iterator<SignificantTerms.Bucket> iterator() {
        return buckets.stream().map(bucket -> (SignificantTerms.Bucket) bucket).collect(Collectors.toList()).iterator();
    }

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(InternalSignificantTerms.Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    protected long getSubsetSize() {
        return subsetSize;
    }

    @Override
    protected long getSupersetSize() {
        return supersetSize;
    }

    @Override
    protected SignificanceHeuristic getSignificanceHeuristic() {
        return significanceHeuristic;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalMappedSignificantTerms<?, ?> that = (InternalMappedSignificantTerms<?, ?>) obj;
        return Objects.equals(format, that.format)
            && subsetSize == that.subsetSize
            && supersetSize == that.supersetSize
            && Objects.equals(significanceHeuristic, that.significanceHeuristic)
            && Objects.equals(buckets, that.buckets)
            && Objects.equals(bucketMap, that.bucketMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, subsetSize, supersetSize, significanceHeuristic, buckets, bucketMap);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), subsetSize);
        builder.field(BG_COUNT, supersetSize);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            // There is a condition (presumably when only one shard has a bucket?) where reduce is not called
            // and I end up with buckets that contravene the user's min_doc_count criteria in my reducer
            if (bucket.subsetDf >= minDocCount) {
                bucket.toXContent(builder, params);
            }
        }
        builder.endArray();
        return builder;
    }
}
