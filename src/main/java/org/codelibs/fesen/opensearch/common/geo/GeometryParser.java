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

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.MapXContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.geometry.Geometry;
import org.codelibs.fesen.opensearch.geometry.GeometryCollection;
import org.codelibs.fesen.opensearch.geometry.Point;
import org.codelibs.fesen.opensearch.geometry.utils.GeometryValidator;
import org.codelibs.fesen.opensearch.geometry.utils.StandardValidator;
import org.codelibs.fesen.opensearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 *
 * @opensearch.internal
 */
public final class GeometryParser {

    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;
    private final boolean ignoreZValue;

    public GeometryParser(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
        GeometryValidator validator = new StandardValidator(ignoreZValue);
        geoJsonParser = new GeoJson(rightOrientation, coerce, validator);
        wellKnownTextParser = new WellKnownText(coerce, validator);
        this.ignoreZValue = ignoreZValue;
    }

    /**
     * Parses supplied XContent into Geometry
     */
    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        return geometryFormat(parser).fromXContent(parser);
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     * This method automatically recognizes the format by examining the provided {@link XContentParser}.
     */
    public GeometryFormat<Geometry> geometryFormat(XContentParser parser) {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            // We don't know the format of the original geometry - so going with default
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else {
            throw new OpenSearchParseException("shape must be an object consisting of type and coordinates");
        }
    }
}
