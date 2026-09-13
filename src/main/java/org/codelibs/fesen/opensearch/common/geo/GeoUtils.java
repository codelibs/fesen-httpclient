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

import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.util.SloppyMath;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.unit.DistanceUnit;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues;
import org.codelibs.fesen.opensearch.core.xcontent.MapXContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentSubParser;
import org.codelibs.fesen.opensearch.geometry.ShapeType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

/**
 * Useful geo utilities
 *
 * @opensearch.internal
 */
public class GeoUtils {
    private static final String ERR_MSG_INVALID_TOKEN = "token [{}] not allowed";
    private static final String ERR_MSG_INVALID_FIELDS = "field must be either [lon|lat], [type|coordinates], or [geohash]";
    /** Maximum valid latitude in degrees. */
    public static final double MAX_LAT = 90.0;
    /** Minimum valid latitude in degrees. */
    public static final double MIN_LAT = -90.0;
    /** Maximum valid longitude in degrees. */
    public static final double MAX_LON = 180.0;
    /** Minimum valid longitude in degrees. */
    public static final double MIN_LON = -180.0;

    public static final String LATITUDE = "lat";
    public static final String LONGITUDE = "lon";
    public static final String GEOHASH = "geohash";

    public static final String GEOJSON_TYPE = "type";
    public static final String GEOJSON_COORDS = "coordinates";

    /**  Error messages for WKT parsing */
    public static final String WKT_BOUNDING_BOX_PARSE_ERROR = "failed to parse WKT bounding box";
    public static final String WKT_BOUNDING_BOX_TYPE_ERROR = "failed to parse WKT bounding box. [%s] found. expected [%s]";
    /** Earth ellipsoid major axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MAJOR_AXIS = 6378137.0;      // meters (WGS 84)

    /** Earth ellipsoid minor axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MINOR_AXIS = 6356752.314245; // meters (WGS 84)

    /** Earth mean radius defined by WGS 84 in meters */
    public static final double EARTH_MEAN_RADIUS = 6371008.7714D;      // meters (WGS 84)

    /** Earth axis ratio defined by WGS 84 (0.996647189335) */
    public static final double EARTH_AXIS_RATIO = EARTH_SEMI_MINOR_AXIS / EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid equator length in meters */
    public static final double EARTH_EQUATOR = 2 * Math.PI * EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid polar distance in meters */
    public static final double EARTH_POLAR_DISTANCE = Math.PI * EARTH_SEMI_MINOR_AXIS;

    /** rounding error for quantized latitude and longitude values */
    public static final double TOLERANCE = 1E-6;

    /** Returns true if latitude is actually a valid latitude value.*/
    public static boolean isValidLatitude(double latitude) {
        if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < GeoUtils.MIN_LAT || latitude > GeoUtils.MAX_LAT) {
            return false;
        }
        return true;
    }

    /** Returns true if longitude is actually a valid longitude value. */
    public static boolean isValidLongitude(double longitude) {
        if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < GeoUtils.MIN_LON || longitude > GeoUtils.MAX_LON) {
            return false;
        }
        return true;
    }

    /**
     * Calculate the width (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double quadTreeCellWidth(int level) {
        assert level >= 0;
        return EARTH_EQUATOR / (1L << level);
    }

    /**
     * Calculate the height (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double quadTreeCellHeight(int level) {
        assert level >= 0;
        return EARTH_POLAR_DISTANCE / (1L << level);
    }

    /**
     * Calculate the size (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double quadTreeCellSize(int level) {
        assert level >= 0;
        return Math.sqrt(EARTH_POLAR_DISTANCE * EARTH_POLAR_DISTANCE + EARTH_EQUATOR * EARTH_EQUATOR) / (1L << level);
    }

    /**
     * Normalize longitude to lie within the -180 (exclusive) to 180 (inclusive) range.
     *
     * @param lon Longitude to normalize
     * @return The normalized longitude.
     */
    public static double normalizeLon(double lon) {
        if (lon > 180d || lon <= -180d) {
            lon = centeredModulus(lon, 360);
        }
        // avoid -0.0
        return lon + 0d;
    }

    /**
     * Normalize the geo {@code Point} for its coordinates to lie within their
     * respective normalized ranges.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     *
     * @param point The point to normalize in-place.
     */
    public static void normalizePoint(GeoPoint point) {
        normalizePoint(point, true, true);
    }

    /**
     * Normalize the geo {@code Point} for the given coordinates to lie within
     * their respective normalized ranges.
     * <p>
     * You can control which coordinate gets normalized with the two flags.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     * If normalizing latitude but not longitude, it is assumed that
     * the longitude is in the form x+k*360, with x in ]-180;180],
     * and k is meaningful to the application.
     * Therefore x will be adjusted while keeping k preserved.
     *
     * @param point   The point to normalize in-place.
     * @param normLat Whether to normalize latitude or leave it as is.
     * @param normLon Whether to normalize longitude.
     */
    public static void normalizePoint(GeoPoint point, boolean normLat, boolean normLon) {
        double[] pt = { point.lon(), point.lat() };
        normalizePoint(pt, normLon, normLat);
        point.reset(pt[1], pt[0]);
    }

    public static void normalizePoint(double[] lonLat) {
        normalizePoint(lonLat, true, true);
    }

    public static void normalizePoint(double[] lonLat, boolean normLon, boolean normLat) {
        assert lonLat != null && lonLat.length == 2;

        normLat = normLat && (lonLat[1] > 90 || lonLat[1] < -90);
        normLon = normLon && (lonLat[0] > 180 || lonLat[0] < -180 || normLat);

        if (normLat) {
            lonLat[1] = centeredModulus(lonLat[1], 360);
            boolean shift = true;
            if (lonLat[1] < -90) {
                lonLat[1] = -180 - lonLat[1];
            } else if (lonLat[1] > 90) {
                lonLat[1] = 180 - lonLat[1];
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    lonLat[0] += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    lonLat[0] += normalizeLon(lonLat[0]) > 0 ? -180 : 180;
                }
            }
        }
        if (normLon) {
            lonLat[0] = centeredModulus(lonLat[0], 360);
        }
    }

    public static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0) {
            rtn += divisor;
        }
        if (rtn > divisor / 2) {
            rtn -= divisor;
        }
        return rtn;
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}:
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(XContentParser parser) throws IOException, OpenSearchParseException {
        return parseGeoPoint(parser, new GeoPoint());
    }

    public static GeoPoint parseGeoPoint(XContentParser parser, GeoPoint point) throws IOException, OpenSearchParseException {
        return parseGeoPoint(parser, point, false);
    }

    /**
     * Represents the point of the geohash cell that should be used as the value of geohash
     *
     * @opensearch.internal
     */
    public enum EffectivePoint {
        TOP_LEFT,
        TOP_RIGHT,
        BOTTOM_LEFT,
        BOTTOM_RIGHT
    }

    /**
     * Parse a geopoint represented as an object, string or an array. If the geopoint is represented as a geohash,
     * the left bottom corner of the geohash cell is used as the geopoint coordinates.GeoBoundingBoxQueryBuilder.java
     */
    public static GeoPoint parseGeoPoint(XContentParser parser, GeoPoint point, final boolean ignoreZValue) throws IOException,
        OpenSearchParseException {
        return parseGeoPoint(parser, point, ignoreZValue, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}. A geopoint has one of the following forms:
     *
     * <ul>
     *     <li>Object: <pre>{@code {"lat": <latitude>, "lon": <longitude}}</pre></li>
     *     <li>String: <pre>{@code "<latitude>,<longitude>"}</pre></li>
     *     <li>GeoHash: <pre>{@code "<geohash>"}</pre></li>
     *     <li>WKT: <pre>{@code "POINT (<longitude> <latitude>)"}</pre></li>
     *     <li>Array: <pre>{@code [<longitude>, <latitude>]}</pre></li>
     *     <li>GeoJson: <pre>{@code {"type": "Point", "coordinates": [<longitude>, <latitude>]}}</pre><li>
     * </ul>
     *
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param point A {@link GeoPoint} that will be reset by the values parsed
     * @param ignoreZValue tells to ignore z value or throw exception when there is a z value
     * @param effectivePoint tells which point to use for GeoHash form
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(
        final XContentParser parser,
        final GeoPoint point,
        final boolean ignoreZValue,
        final EffectivePoint effectivePoint
    ) throws IOException, OpenSearchParseException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                parseGeoPointObject(parser, point, ignoreZValue, effectivePoint);
                break;
            case START_ARRAY:
                parseGeoPointArray(parser, point, ignoreZValue);
                break;
            case VALUE_STRING:
                String val = parser.text();
                point.resetFromString(val, ignoreZValue, effectivePoint);
                break;
            default:
                throw new OpenSearchParseException("geo_point expected");
        }
        return point;
    }

    private static GeoPoint parseGeoPointObject(
        final XContentParser parser,
        final GeoPoint point,
        final boolean ignoreZValue,
        final GeoUtils.EffectivePoint effectivePoint
    ) throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            if (subParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, subParser.currentToken());
            }

            String field = subParser.currentName();
            if (LONGITUDE.equals(field) || LATITUDE.equals(field)) {
                parseGeoPointObjectBasicFields(subParser, point);
            } else if (GEOHASH.equals(field)) {
                parseGeoHashFields(subParser, point, effectivePoint);
            } else if (GEOJSON_TYPE.equals(field) || GEOJSON_COORDS.equals(field)) {
                parseGeoJsonFields(subParser, point, ignoreZValue);
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            if (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            return point;
        }
    }

    private static GeoPoint parseGeoPointObjectBasicFields(final XContentParser parser, final GeoPoint point) throws IOException {
        HashMap<String, Double> data = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                break;
            }

            String field = parser.currentName();
            if (LONGITUDE.equals(field) == false && LATITUDE.equals(field) == false) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
            switch (parser.nextToken()) {
                case VALUE_NUMBER:
                case VALUE_STRING:
                    try {
                        data.put(field, parser.doubleValue(true));
                    } catch (NumberFormatException e) {
                        throw new OpenSearchParseException("[{}] and [{}] must be valid double values", e, LONGITUDE, LATITUDE);
                    }
                    break;
                default:
                    throw new OpenSearchParseException("{} must be a number", field);
            }
        }

        if (data.get(LONGITUDE) == null) {
            throw new OpenSearchParseException("field [{}] missing", LONGITUDE);
        }
        if (data.get(LATITUDE) == null) {
            throw new OpenSearchParseException("field [{}] missing", LATITUDE);
        }

        return point.reset(data.get(LATITUDE), data.get(LONGITUDE));
    }

    private static GeoPoint parseGeoHashFields(
        final XContentParser parser,
        final GeoPoint point,
        final GeoUtils.EffectivePoint effectivePoint
    ) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, parser.currentToken());
        }

        if (GEOHASH.equals(parser.currentName()) == false) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
        }

        if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
            throw new OpenSearchParseException("{} must be a string", GEOHASH);
        }

        return point.parseGeoHash(parser.text(), effectivePoint);
    }

    private static GeoPoint parseGeoJsonFields(final XContentParser parser, final GeoPoint point, final boolean ignoreZValue)
        throws IOException {
        boolean hasTypePoint = false;
        boolean hasCoordinates = false;
        for (int i = 0; i < 2; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                if (hasTypePoint == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_TYPE);
                }
                if (hasCoordinates == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_COORDS);
                }
            }

            if (GEOJSON_TYPE.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw new OpenSearchParseException("{} must be a string", GEOJSON_TYPE);
                }

                // To be consistent with geo_shape parsing, ignore case here as well.
                if (ShapeType.POINT.name().equalsIgnoreCase(parser.text()) == false) {
                    throw new OpenSearchParseException("{} must be Point", GEOJSON_TYPE);
                }
                hasTypePoint = true;
            } else if (GEOJSON_COORDS.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new OpenSearchParseException("{} must be an array", GEOJSON_COORDS);
                }
                parseGeoPointArray(parser, point, ignoreZValue);
                hasCoordinates = true;
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
        }

        return point;
    }

    private static GeoPoint parseGeoPointArray(final XContentParser parser, final GeoPoint point, final boolean ignoreZValue)
        throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            double x = Double.NaN;
            double y = Double.NaN;

            int element = 0;
            while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                    throw new OpenSearchParseException("numeric value expected");
                }
                element++;
                if (element == 1) {
                    x = parser.doubleValue();
                } else if (element == 2) {
                    y = parser.doubleValue();
                } else if (element == 3) {
                    GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
                } else {
                    throw new OpenSearchParseException("[geo_point] field type does not accept more than 3 values");
                }
            }

            if (element < 2) {
                throw new OpenSearchParseException("[geo_point] field type should have at least two dimensions");
            }
            return point.reset(y, x);
        }
    }

    /**
     * Parse a {@link GeoPoint} from a string. The string must have one of the following forms:
     *
     * <ul>
     *     <li>Latitude, Longitude form: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash form:: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     * </ul>
     *
     * @param val a String to parse the value from
     * @return new parsed {@link GeoPoint}
     */
    public static GeoPoint parseFromString(String val) {
        GeoPoint point = new GeoPoint();
        return point.resetFromString(val, false, EffectivePoint.BOTTOM_LEFT);
    }

    private GeoUtils() {}
}
