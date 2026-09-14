/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.bucket.composite;

import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A functional interface which encapsulates the parsing function to be called for the aggregation which is
 * also registered as CompositeAggregation.
 */
@FunctionalInterface
public interface CompositeAggregationParsingFunction {
    /**
     * Parses this instance.
     *
     * @param name the name
     * @param parser the parser
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    CompositeValuesSourceBuilder<?> parse(final String name, final XContentParser parser) throws IOException;
}
