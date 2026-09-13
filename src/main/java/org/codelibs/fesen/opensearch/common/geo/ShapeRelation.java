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

package org.codelibs.fesen.opensearch.common.geo;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Enum representing the relationship between a Query / Filter Shape and indexed Shapes
 * that will be used to determine if a Document should be matched or not
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum ShapeRelation implements Writeable {

    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    WITHIN("within"),
    CONTAINS("contains");

    private final String relationName;

    ShapeRelation(String relationName) {
        this.relationName = relationName;
    }

    public static ShapeRelation readFromStream(StreamInput in) throws IOException {
        return in.readEnum(ShapeRelation.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static ShapeRelation getRelationByName(String name) {
        name = name.toLowerCase(Locale.ENGLISH);
        for (ShapeRelation relation : ShapeRelation.values()) {
            if (relation.relationName.equals(name)) {
                return relation;
            }
        }
        return null;
    }

    public String getRelationName() {
        return relationName;
    }
}
