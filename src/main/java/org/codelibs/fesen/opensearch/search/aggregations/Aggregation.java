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

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;

import java.util.Map;

/**
 * An aggregation. Extends {@link ToXContent} as it makes it easier to print out its content.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface Aggregation extends ToXContentFragment {

    /**
     * Delimiter used when prefixing aggregation names with their type
     * using the typed_keys parameter
     */
    String TYPED_KEYS_DELIMITER = "#";

    /**
     * Returns the name.
     *
     * @return The name of this aggregation.
     */
    String getName();

    /**
     * Returns the type.
     *
     * @return a string representing the type of the aggregation. This type is added to
     * the aggregation name in the response, so that it can later be used by clients
     * to determine type of the aggregation and parse it into the proper object.
     */
    String getType();

    /**
     * Get the optional byte array metadata that was set on the aggregation
     *
     * @return the metadata
     */
    Map<String, Object> getMetadata();

    /**
     * Common xcontent fields that are shared among addAggregation
     */
    final class CommonFields extends ParseField.CommonFields {
        /**
         * Creates a new CommonFields.
         */
        CommonFields() {
        }

        /**
         * The META constant.
         */
        public static final ParseField META = new ParseField("meta");
        /**
         * The BUCKETS constant.
         */
        public static final ParseField BUCKETS = new ParseField("buckets");
        /**
         * The VALUE constant.
         */
        public static final ParseField VALUE = new ParseField("value");
        /**
         * The VALUES constant.
         */
        public static final ParseField VALUES = new ParseField("values");
        /**
         * The VALUE_AS_STRING constant.
         */
        public static final ParseField VALUE_AS_STRING = new ParseField("value_as_string");
        /**
         * The DOC_COUNT constant.
         */
        public static final ParseField DOC_COUNT = new ParseField("doc_count");
        /**
         * The KEY constant.
         */
        public static final ParseField KEY = new ParseField("key");
        /**
         * The KEY_AS_STRING constant.
         */
        public static final ParseField KEY_AS_STRING = new ParseField("key_as_string");
        /**
         * The FROM constant.
         */
        public static final ParseField FROM = new ParseField("from");
        /**
         * The FROM_AS_STRING constant.
         */
        public static final ParseField FROM_AS_STRING = new ParseField("from_as_string");
        /**
         * The TO constant.
         */
        public static final ParseField TO = new ParseField("to");
        /**
         * The TO_AS_STRING constant.
         */
        public static final ParseField TO_AS_STRING = new ParseField("to_as_string");
        /**
         * The MIN constant.
         */
        public static final ParseField MIN = new ParseField("min");
        /**
         * The MIN_AS_STRING constant.
         */
        public static final ParseField MIN_AS_STRING = new ParseField("min_as_string");
        /**
         * The MAX constant.
         */
        public static final ParseField MAX = new ParseField("max");
        /**
         * The MAX_AS_STRING constant.
         */
        public static final ParseField MAX_AS_STRING = new ParseField("max_as_string");
    }
}
