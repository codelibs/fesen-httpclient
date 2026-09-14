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

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.geo.GeoUtils.EffectivePoint;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.geometry.Geometry;
import org.codelibs.fesen.opensearch.geometry.Point;
import org.codelibs.fesen.opensearch.geometry.Rectangle;
import org.codelibs.fesen.opensearch.geometry.ShapeType;
import org.codelibs.fesen.opensearch.geometry.utils.GeographyValidator;
import org.codelibs.fesen.opensearch.geometry.utils.Geohash;
import org.codelibs.fesen.opensearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;


/**
 * Core geo point
 *
 * @opensearch.internal
 */
public class GeoPoint implements ToXContentFragment {

    /**
     * The lat.
     */
    protected double lat;
    /**
     * The lon.
     */
    protected double lon;

    /**
     * Creates a new GeoPoint.
     */
    public GeoPoint() {}

    /**
     * Creates a new GeoPoint.
     *
     * @param lat the lat
     * @param lon the lon
     */
    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    /**
     * Creates a new GeoPoint.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public GeoPoint(final StreamInput in) throws IOException {
        this.lat = in.readDouble();
        this.lon = in.readDouble();
    }

    /**
     * Resets this instance.
     *
     * @param lat the lat
     * @param lon the lon
     * @return this instance
     */
    public GeoPoint reset(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    /**
     * Resets the lat.
     *
     * @param lat the lat
     * @return this instance
     */
    public GeoPoint resetLat(double lat) {
        this.lat = lat;
        return this;
    }

    /**
     * Resets the lon.
     *
     * @param lon the lon
     * @return this instance
     */
    public GeoPoint resetLon(double lon) {
        this.lon = lon;
        return this;
    }

    /**
     * Resets the from string.
     *
     * @param value the value
     * @return this instance
     */
    public GeoPoint resetFromString(String value) {
        return resetFromString(value, false, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Resets the from string.
     *
     * @param value the value
     * @param ignoreZValue the ignore z value
     * @param effectivePoint the effective point
     * @return this instance
     */
    public GeoPoint resetFromString(String value, final boolean ignoreZValue, EffectivePoint effectivePoint) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else if (value.contains(",")) {
            return resetFromCoordinates(value, ignoreZValue);
        }
        return parseGeoHash(value, effectivePoint);
    }

    /**
     * Resets the from coordinates.
     *
     * @param value the value
     * @param ignoreZValue the ignore z value
     * @return this instance
     */
    public GeoPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3) {
            throw new OpenSearchParseException(
                "failed to parse [{}], expected 2 or 3 coordinates " + "but found: [{}]",
                value,
                vals.length
            );
        }
        final double lat;
        final double lon;
        try {
            lat = Double.parseDouble(vals[0].trim());
        } catch (NumberFormatException ex) {
            throw new OpenSearchParseException("latitude must be a number");
        }
        try {
            lon = Double.parseDouble(vals[1].trim());
        } catch (NumberFormatException ex) {
            throw new OpenSearchParseException("longitude must be a number");
        }
        if (vals.length > 2) {
            GeoPoint.assertZValue(ignoreZValue, Double.parseDouble(vals[2].trim()));
        }
        return reset(lat, lon);
    }

    private GeoPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = new WellKnownText(false, new GeographyValidator(ignoreZValue)).fromWKT(value);
        } catch (Exception e) {
            throw new OpenSearchParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new OpenSearchParseException("[geo_point] supports only POINT among WKT primitives, " + "but found " + geometry.type());
        }
        Point point = (Point) geometry;
        return reset(point.getY(), point.getX());
    }

    GeoPoint parseGeoHash(String geohash, EffectivePoint effectivePoint) {
        if (effectivePoint == EffectivePoint.BOTTOM_LEFT) {
            return resetFromGeoHash(geohash);
        } else {
            Rectangle rectangle = Geohash.toBoundingBox(geohash);
            switch (effectivePoint) {
                case TOP_LEFT:
                    return reset(rectangle.getMaxY(), rectangle.getMinX());
                case TOP_RIGHT:
                    return reset(rectangle.getMaxY(), rectangle.getMaxX());
                case BOTTOM_RIGHT:
                    return reset(rectangle.getMinY(), rectangle.getMaxX());
                default:
                    throw new IllegalArgumentException("Unsupported effective point " + effectivePoint);
            }
        }
    }

    // todo this is a crutch because LatLonPoint doesn't have a helper for returning .stringValue()

    /**
     * Resets the from geo hash.
     *
     * @param geohash the geohash
     * @return this instance
     */
    public GeoPoint resetFromGeoHash(String geohash) {
        final long hash;
        try {
            hash = Geohash.mortonEncode(geohash);
        } catch (IllegalArgumentException ex) {
            throw new OpenSearchParseException(ex.getMessage(), ex);
        }
        return this.reset(Geohash.decodeLatitude(hash), Geohash.decodeLongitude(hash));
    }

    /**
     * Writes this instance to the given output.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeDouble(this.lat);
        out.writeDouble(this.lon);
    }

    /**
     * Returns the lat.
     *
     * @return the lat
     */
    public double lat() {
        return this.lat;
    }

    /**
     * Returns the lat.
     *
     * @return the lat
     */
    public double getLat() {
        return this.lat;
    }

    /**
     * Returns the lon.
     *
     * @return the lon
     */
    public double lon() {
        return this.lon;
    }

    /**
     * Returns the lon.
     *
     * @return the lon
     */
    public double getLon() {
        return this.lon;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoPoint geoPoint = (GeoPoint) o;

        if (Double.compare(geoPoint.lat, lat) != 0) return false;
        if (Double.compare(geoPoint.lon, lon) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = Long.hashCode(temp);
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + Long.hashCode(temp);
        return result;
    }

    @Override
    public String toString() {
        return lat + ", " + lon;
    }

    /**
     * Creates an instance from geohash.
     *
     * @param geohash the geohash
     * @return the new geohash
     */
    public static GeoPoint fromGeohash(String geohash) {
        return new GeoPoint().resetFromGeoHash(geohash);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.latlon(lat, lon);
    }

    /**
     * Returns the assert z value.
     *
     * @param ignoreZValue the ignore z value
     * @param zValue the z value
     * @return the assert z value
     */
    public static double assertZValue(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new OpenSearchParseException(
                "Exception parsing coordinates: found Z value [{}] but [{}] " + "parameter is [{}]",
                zValue,
                "ignore_z_value",
                ignoreZValue
            );
        }
        return zValue;
    }
}
