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

package org.codelibs.fesen.opensearch.search.aggregations;

import org.codelibs.fesen.opensearch.common.CheckedBiConsumer;
import org.codelibs.fesen.opensearch.common.CheckedFunction;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A multi-bucket agg that has been parsed
 *
 * @param <B> the builder type
 * @opensearch.internal
 */
public abstract class ParsedMultiBucketAggregation<B extends ParsedMultiBucketAggregation.Bucket> extends ParsedAggregation
    implements
        MultiBucketsAggregation {
            /**
             * Creates a new ParsedMultiBucketAggregation.
             */
            public ParsedMultiBucketAggregation() {
            }

    /**
     * The buckets.
     */
    protected final List<B> buckets = new ArrayList<>();
    /**
     * The keyed.
     */
    protected boolean keyed = false;

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (B bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    /**
     * Performs the declare multi bucket aggregation fields step.
     *
     * @param objectParser the object parser
     * @param bucketParser the bucket parser
     * @param keyedBucketParser the keyed bucket parser
     */
    protected static void declareMultiBucketAggregationFields(
        final ObjectParser<? extends ParsedMultiBucketAggregation, Void> objectParser,
        final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser,
        final CheckedFunction<XContentParser, ParsedBucket, IOException> keyedBucketParser
    ) {
        declareAggregationFields(objectParser);
        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.keyed = true;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    aggregation.buckets.add(keyedBucketParser.apply(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.keyed = false;
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    aggregation.buckets.add(bucketParser.apply(parser));
                }
            }
        }, CommonFields.BUCKETS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    /**
     * A parsed bucket
     *
     * @opensearch.internal
     */
    public abstract static class ParsedBucket implements MultiBucketsAggregation.Bucket {
        /**
         * Creates a new ParsedBucket.
         */
        public ParsedBucket() {
        }

        private Aggregations aggregations;
        private String keyAsString;
        private long docCount;
        private boolean keyed;

        /**
         * Sets the key as string.
         *
         * @param keyAsString the key as string
         */
        protected void setKeyAsString(String keyAsString) {
            this.keyAsString = keyAsString;
        }

        @Override
        public String getKeyAsString() {
            return keyAsString;
        }

        /**
         * Sets the doc count.
         *
         * @param docCount the doc count
         */
        protected void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        /**
         * Sets the keyed.
         *
         * @param keyed the keyed
         */
        public void setKeyed(boolean keyed) {
            this.keyed = keyed;
        }

        /**
         * Returns the keyed flag.
         *
         * @return the keyed flag
         */
        protected boolean isKeyed() {
            return keyed;
        }

        /**
         * Sets the aggregations.
         *
         * @param aggregations the aggregations
         */
        protected void setAggregations(Aggregations aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                // Subclasses can override the getKeyAsString method to handle specific cases like
                // keyed bucket with RAW doc value format where the key_as_string field is not printed
                // out but we still need to have a string version of the key to use as the bucket's name.
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            if (keyAsString != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        /**
         * Returns the key to XContent.
         *
         * @param builder the content builder
         * @return the key to XContent
         * @throws IOException if an I/O error occurs
         */
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        /**
         * Parses the XContent.
         *
         * @param <B> the builder type
         * @param parser the parser
         * @param keyed the keyed
         * @param bucketSupplier the bucket supplier
         * @param keyConsumer the key consumer
         * @return this instance
         * @throws IOException if an I/O error occurs
         */
        protected static <B extends ParsedBucket> B parseXContent(
            final XContentParser parser,
            final boolean keyed,
            final Supplier<B> bucketSupplier,
            final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer
        ) throws IOException {
            final B bucket = bucketSupplier.get();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else {
                        XContentParserUtils.parseTypedKeysObject(
                            parser,
                            Aggregation.TYPED_KEYS_DELIMITER,
                            Aggregation.class,
                            aggregations::add
                        );
                    }
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }
}
