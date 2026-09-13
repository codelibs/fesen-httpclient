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

package org.codelibs.fesen.opensearch.search.aggregations.bucket;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.SloppyMath;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.geo.GeoPoint;
import org.codelibs.fesen.opensearch.common.util.OpenSearchSloppyMath;
import org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser.ValueType;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.geometry.Rectangle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.codelibs.fesen.opensearch.common.geo.GeoUtils.normalizeLon;

/**
 * Implements geotile key hashing, same as used by many map tile implementations.
 * The string key is formatted as  "zoom/x/y"
 * The hash value (long) contains all three of those values compacted into a single 64bit value:
 *   bits 58..63 -- zoom (0..29)
 *   bits 29..57 -- X tile index (0..2^zoom)
 *   bits  0..28 -- Y tile index (0..2^zoom)
 *
 * @opensearch.internal
 */
public final class GeoTileUtils {

    private GeoTileUtils() {}

    private static final double PI_DIV_2 = Math.PI / 2;

    /**
     * Largest number of tiles (precision) to use.
     * This value cannot be more than (64-5)/2 = 29, because 5 bits are used for zoom level itself (0-31)
     * If zoom is not stored inside hash, it would be possible to use up to 32.
     * Note that changing this value will make serialization binary-incompatible between versions.
     * Another consideration is that index optimizes lat/lng storage, loosing some precision.
     * E.g. hash lng=140.74779717298918D lat=45.61884022447444D == "18/233561/93659", but shown as "18/233561/93658"
     */
    public static final int MAX_ZOOM = 29;

    /**
     * The geo-tile map is clipped at 85.05112878 to 90 and -85.05112878 to -90
     */
    public static final double LATITUDE_MASK = 85.0511287798066;

    /**
     * Since shapes are encoded, their boundaries are to be compared to against the encoded/decoded values of <code>LATITUDE_MASK</code>
     */
    public static final double NORMALIZED_LATITUDE_MASK = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(LATITUDE_MASK));
    public static final double NORMALIZED_NEGATIVE_LATITUDE_MASK = GeoEncodingUtils.decodeLatitude(
        GeoEncodingUtils.encodeLatitude(-LATITUDE_MASK)
    );

    /**
     * Bit position of the zoom value within hash - zoom is stored in the most significant 6 bits of a long number.
     */
    private static final int ZOOM_SHIFT = MAX_ZOOM * 2;

    /**
     * Bit mask to extract just the lowest 29 bits of a long
     */
    private static final long X_Y_VALUE_MASK = (1L << MAX_ZOOM) - 1;

    /**
     * Assert the precision value is within the allowed range, and return it if ok, or throw.
     */
    public static int checkPrecisionRange(int precision) {
        if (precision < 0 || precision > MAX_ZOOM) {
            throw new IllegalArgumentException(
                "Invalid geotile_grid precision of " + precision + ". Must be between 0 and " + MAX_ZOOM + "."
            );
        }
        return precision;
    }

    /**
     * Parse geotile hash as zoom, x, y integers.
     */
    private static int[] parseHash(long hash) {
        final int zoom = (int) (hash >>> ZOOM_SHIFT);
        final int xTile = (int) ((hash >>> MAX_ZOOM) & X_Y_VALUE_MASK);
        final int yTile = (int) (hash & X_Y_VALUE_MASK);
        return new int[] { zoom, xTile, yTile };
    }

    /**
     * Encode to a geotile string from the geotile based long format
     */
    public static String stringEncode(long hash) {
        int[] res = parseHash(hash);
        validateZXY(res[0], res[1], res[2]);
        return "" + res[0] + "/" + res[1] + "/" + res[2];
    }

    /**
     * Validates Zoom, X, and Y values, and returns the total number of allowed tiles along the x/y axis.
     */
    private static int validateZXY(int zoom, int xTile, int yTile) {
        final int tiles = 1 << checkPrecisionRange(zoom);
        if (xTile < 0 || yTile < 0 || xTile >= tiles || yTile >= tiles) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", zoom, xTile, yTile)
            );
        }
        return tiles;
    }
}
