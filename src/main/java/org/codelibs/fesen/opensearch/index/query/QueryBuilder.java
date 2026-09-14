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

package org.codelibs.fesen.opensearch.index.query;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * Foundation class for all OpenSearch query builders
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface QueryBuilder extends NamedWriteable, ToXContentObject, Rewriteable<QueryBuilder> {

    /**
     * This function combines a filter with a query builder. If the query builder itself has
     * a filter we will combine the filter and return the query builder itself.
     * If not we will use a bool query builder to combine the query builder and
     * the filter and then return the bool query builder.
     * If the filter is null we simply return the query builder without any operation.
     *
     * @param filter The null filter to be added to the existing filter.
     * @return A QueryBuilder with the filter added to the existing filter.
     */
    QueryBuilder filter(QueryBuilder filter);

    /**
     * Sets the arbitrary name to be assigned to the query (see named queries).
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     *
     * @param queryName the query name
     * @return this instance
     */
    QueryBuilder queryName(String queryName);

    /**
     * Returns the arbitrary name assigned to the query (see named queries).
     *
     * @return this instance
     */
    String queryName();

    /**
     * Returns the boost for this query.
     *
     * @return this instance
     */
    float boost();

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     *
     * @param boost the boost
     * @return this instance
     */
    QueryBuilder boost(float boost);

    /**
     * Returns the name that identifies uniquely the query
     *
     * @return the name
     */
    String getName();

    /**
     * Rewrites this query builder into its primitive form. By default this method return the builder itself. If the builder
     * did not change the identity reference must be returned otherwise the builder will be rewritten infinitely.
     */
    @Override
    default QueryBuilder rewrite(QueryRewriteContext queryShardContext) throws IOException {
        return this;
    }

    /**
     * Recurse through the QueryBuilder tree, visiting any child QueryBuilder.
     * @param visitor a query builder visitor to be called by each query builder in the tree.
     */
    default void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
    };

    /**
     * Indicates whether this query benefits from intra-segment search.
     * Override to return {@code true} for compute-heavy queries that parallelize well
     * Default is {@code false} - queries must explicitly opt-in.
     *
     * @return the supports intra segment search
     */
    default boolean supportsIntraSegmentSearch() {
        return false;
    }

}
