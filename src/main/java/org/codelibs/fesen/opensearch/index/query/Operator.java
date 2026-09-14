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

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Locale;

/**
 * Boolean operators
 *
 * @opensearch.internal
 */
public enum Operator implements Writeable {
    /**
     * The OR value.
     */
    OR,
    /**
     * The AND value.
     */
    AND;

    /**
     * Returns this instance as boolean clause occur.
     *
     * @return the boolean clause occur
     */
    public BooleanClause.Occur toBooleanClauseOccur() {
        switch (this) {
            case OR:
                return BooleanClause.Occur.SHOULD;
            case AND:
                return BooleanClause.Occur.MUST;
            default:
                throw Operator.newOperatorException(this.toString());
        }
    }

    /**
     * Reads the from stream.
     *
     * @param in the input to read from
     * @return the from stream
     * @throws IOException if an I/O error occurs
     */
    public static Operator readFromStream(StreamInput in) throws IOException {
        return in.readEnum(Operator.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    /**
     * Creates an instance from string.
     *
     * @param op the op
     * @return the new string
     */
    public static Operator fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    /**
     * Creates a new operator exception.
     *
     * @param op the op
     * @return the new operator exception
     */
    public static IllegalArgumentException newOperatorException(String op) {
        return new IllegalArgumentException(
            "operator needs to be either " + CollectionUtils.arrayAsArrayList(values()) + ", but not [" + op + "]"
        );
    }
}
