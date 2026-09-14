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

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser.ValueType;
import org.codelibs.fesen.opensearch.search.aggregations.ParsedAggregation;

/**
 * A single value numeric agg result parsed between nodes
 *
 * @opensearch.internal
 */
public abstract class ParsedSingleValueNumericMetricsAggregation extends ParsedAggregation
    implements
        NumericMetricsAggregation.SingleValue {
            /**
             * Creates a new ParsedSingleValueNumericMetricsAggregation.
             */
            public ParsedSingleValueNumericMetricsAggregation() {
            }

    /**
     * The value.
     */
    protected double value;
    /**
     * The value as string.
     */
    protected String valueAsString;

    @Override
    public String getValueAsString() {
        if (valueAsString != null) {
            return valueAsString;
        } else {
            return Double.toString(value);
        }
    }

    @Override
    public double value() {
        return value;
    }

    /**
     * Sets the value.
     *
     * @param value the value
     */
    protected void setValue(double value) {
        this.value = value;
    }

    /**
     * Sets the value as string.
     *
     * @param valueAsString the value as string
     */
    protected void setValueAsString(String valueAsString) {
        this.valueAsString = valueAsString;
    }

    /**
     * Performs the declare single value fields step.
     *
     * @param objectParser the object parser
     * @param defaultNullValue the default null value
     */
    protected static void declareSingleValueFields(
        ObjectParser<? extends ParsedSingleValueNumericMetricsAggregation, Void> objectParser,
        double defaultNullValue
    ) {
        declareAggregationFields(objectParser);
        objectParser.declareField(
            ParsedSingleValueNumericMetricsAggregation::setValue,
            (parser, context) -> parseDouble(parser, defaultNullValue),
            CommonFields.VALUE,
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareString(ParsedSingleValueNumericMetricsAggregation::setValueAsString, CommonFields.VALUE_AS_STRING);
    }
}
