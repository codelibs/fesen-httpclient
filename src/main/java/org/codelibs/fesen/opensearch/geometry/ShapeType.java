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

package org.codelibs.fesen.opensearch.geometry;

import java.util.Locale;

/**
 * Shape types supported by opensearch
 */
public enum ShapeType {
    /**
     * The POINT value.
     */
    POINT,
    /**
     * The MULTIPOINT value.
     */
    MULTIPOINT,
    /**
     * The LINESTRING value.
     */
    LINESTRING,
    /**
     * The MULTILINESTRING value.
     */
    MULTILINESTRING,
    /**
     * The POLYGON value.
     */
    POLYGON,
    /**
     * The MULTIPOLYGON value.
     */
    MULTIPOLYGON,
    /**
     * The GEOMETRYCOLLECTION value.
     */
    GEOMETRYCOLLECTION,
    /**
     * The linearring.
     */
    LINEARRING, // not serialized by itself in WKT or WKB
    /**
     * The envelope.
     */
    ENVELOPE, // not part of the actual WKB spec
    /**
     * The circle.
     */
    CIRCLE; // not part of the actual WKB spec

    /**
     * Returns the for name.
     *
     * @param shapeName the shape name
     * @return the for name
     */
    public static ShapeType forName(String shapeName) {
        return ShapeType.valueOf(shapeName.toUpperCase(Locale.ROOT));
    }
}
