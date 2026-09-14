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

package org.codelibs.fesen.opensearch.geometry.utils;

import org.codelibs.fesen.opensearch.geometry.Point;
import org.codelibs.fesen.opensearch.geometry.Rectangle;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Utilities for converting to/from the GeoHash standard
 * <p>
 * The geohash long format is represented as lon/lat (x/y) interleaved with the 4 least significant bits
 * representing the level (1-12) [xyxy...xyxyllll]
 * <p>
 * This differs from a morton encoded value which interleaves lat/lon (y/x).
 * <p>
 * NOTE: this will replace {@code org.codelibs.fesen.opensearch.common.geo.GeoHashUtils}
 */
public class Geohash {
    private static final char[] BASE_32 = {
        '0',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'j',
        'k',
        'm',
        'n',
        'p',
        'q',
        'r',
        's',
        't',
        'u',
        'v',
        'w',
        'x',
        'y',
        'z' };

    private static final String BASE_32_STRING = new String(BASE_32);
    /** maximum precision for geohash strings */
    public static final int PRECISION = 12;
    /** number of bits used for quantizing latitude and longitude values */
    private static final short BITS = 32;
    private static final double LAT_SCALE = (0x1L << (BITS - 1)) / 180.0D;
    private static final double LON_SCALE = (0x1L << (BITS - 1)) / 360.0D;

    private static final short MORTON_OFFSET = (BITS << 1) - (PRECISION * 5);
    /** Bit encoded representation of the latitude of north pole */
    private static final long MAX_LAT_BITS = (0x1L << (PRECISION * 5 / 2)) - 1;

    // Below code is adapted from the spatial4j library (GeohashUtils.java) Apache 2.0 Licensed
    private static final double[] precisionToLatHeight, precisionToLonWidth;
    static {
        precisionToLatHeight = new double[PRECISION + 1];
        precisionToLonWidth = new double[PRECISION + 1];
        precisionToLatHeight[0] = 90 * 2;
        precisionToLonWidth[0] = 180 * 2;
        boolean even = false;
        for (int i = 1; i <= PRECISION; i++) {
            precisionToLatHeight[i] = precisionToLatHeight[i - 1] / (even ? 8 : 4);
            precisionToLonWidth[i] = precisionToLonWidth[i - 1] / (even ? 4 : 8);
            even = !even;
        }
    }

    // no instance:
    private Geohash() {}

    /**
     * Returns a {@link Point} instance from a geohash string
     *
     * @param geohash the geohash
     * @return the point
     */
    public static Point toPoint(final String geohash) throws IllegalArgumentException {
        final long hash = mortonEncode(geohash);
        return new Point(decodeLongitude(hash), decodeLatitude(hash));
    }

    /**
     * Computes the bounding box coordinates from a given geohash
     *
     * @param geohash Geohash of the defined cell
     * @return GeoRect rectangle defining the bounding box
     */
    public static Rectangle toBoundingBox(final String geohash) {
        // bottom left is the coordinate
        Point bottomLeft = toPoint(geohash);
        int len = Math.min(12, geohash.length());
        long ghLong = longEncode(geohash, len);
        // shift away the level
        ghLong >>>= 4;
        // deinterleave
        long lon = BitUtil.deinterleave(ghLong >>> 1);
        long lat = BitUtil.deinterleave(ghLong);
        final int shift = (12 - len) * 5 + 2;
        if (lat < MAX_LAT_BITS) {
            // add 1 to lat and lon to get topRight
            ghLong = BitUtil.interleave((int) (lat + 1), (int) (lon + 1)) << 4 | len;
            final long mortonHash = BitUtil.flipFlop((ghLong >>> 4) << shift);
            Point topRight = new Point(decodeLongitude(mortonHash), decodeLatitude(mortonHash));
            return new Rectangle(bottomLeft.getX(), topRight.getX(), topRight.getY(), bottomLeft.getY());
        } else {
            // We cannot go north of north pole, so just using 90 degrees instead of calculating it using
            // add 1 to lon to get lon of topRight, we are going to use 90 for lat
            ghLong = BitUtil.interleave((int) lat, (int) (lon + 1)) << 4 | len;
            final long mortonHash = BitUtil.flipFlop((ghLong >>> 4) << shift);
            Point topRight = new Point(decodeLongitude(mortonHash), decodeLatitude(mortonHash));
            return new Rectangle(bottomLeft.getX(), topRight.getX(), 90D, bottomLeft.getY());
        }
    }

    /**
     * Encode to a geohash string from the geohash based long format
     *
     * @param geoHashLong the geo hash long
     * @return the string encode
     */
    public static final String stringEncode(long geoHashLong) {
        int level = (int) geoHashLong & 15;
        geoHashLong >>>= 4;
        char[] chars = new char[level];
        do {
            chars[--level] = BASE_32[(int) (geoHashLong & 31L)];
            geoHashLong >>>= 5;
        } while (level > 0);

        return new String(chars);
    }

    /**
     * Encode from geohash string to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    private static long longEncode(final String hash, int length) {
        int level = length - 1;
        long b;
        long l = 0L;
        for (char c : hash.toCharArray()) {
            b = (long) (BASE_32_STRING.indexOf(c));
            l |= (b << (level-- * 5));
            if (level < 0) {
                // We cannot handle more than 12 levels
                break;
            }
        }
        return (l << 4) | length;
    }

    /**
     * Encode to a morton long value from a given geohash string
     *
     * @param hash the hash
     * @return the morton encode
     */
    public static long mortonEncode(final String hash) {
        if (hash.isEmpty()) {
            throw new IllegalArgumentException("empty geohash");
        }
        int level = 11;
        long b;
        long l = 0L;
        for (char c : hash.toCharArray()) {
            b = (long) (BASE_32_STRING.indexOf(c));
            if (b < 0) {
                throw new IllegalArgumentException("unsupported symbol [" + c + "] in geohash [" + hash + "]");
            }
            l |= (b << ((level-- * 5) + (MORTON_OFFSET - 2)));
            if (level < 0) {
                // We cannot handle more than 12 levels
                break;
            }
        }
        return BitUtil.flipFlop(l);
    }

    /**
     * decode longitude value from morton encoded geo point
     *
     * @param hash the hash
     * @return this instance
     */
    public static double decodeLongitude(final long hash) {
        return unscaleLon(BitUtil.deinterleave(hash));
    }

    /**
     * decode latitude value from morton encoded geo point
     *
     * @param hash the hash
     * @return this instance
     */
    public static double decodeLatitude(final long hash) {
        return unscaleLat(BitUtil.deinterleave(hash >>> 1));
    }

    private static double unscaleLon(final long val) {
        return (val / LON_SCALE) - 180;
    }

    private static double unscaleLat(final long val) {
        return (val / LAT_SCALE) - 90;
    }
}
