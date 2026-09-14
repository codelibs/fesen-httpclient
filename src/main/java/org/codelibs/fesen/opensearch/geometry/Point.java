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

import org.codelibs.fesen.opensearch.geometry.utils.WellKnownText;

/**
 * Represents a Point on the earth's surface in decimal degrees and optional altitude in meters.
 */
public class Point implements Geometry {
    /**
     * The EMPTY constant.
     */
    public static final Point EMPTY = new Point();

    private final double y;
    private final double x;
    private final double z;
    private final boolean empty;

    private Point() {
        y = 0;
        x = 0;
        z = Double.NaN;
        empty = true;
    }

    /**
     * Creates a new Point.
     *
     * @param x the x
     * @param y the y
     */
    public Point(double x, double y) {
        this(x, y, Double.NaN);
    }

    /**
     * Creates a new Point.
     *
     * @param x the x
     * @param y the y
     * @param z the z
     */
    public Point(double x, double y, double z) {
        this.y = y;
        this.x = x;
        this.z = z;
        this.empty = false;
    }

    @Override
    public ShapeType type() {
        return ShapeType.POINT;
    }

    /**
     * Returns the y.
     *
     * @return the y
     */
    public double getY() {
        return y;
    }

    /**
     * Returns the x.
     *
     * @return the x
     */
    public double getX() {
        return x;
    }

    /**
     * Returns the z.
     *
     * @return the z
     */
    public double getZ() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point point = (Point) o;
        if (point.empty != empty) return false;
        if (Double.compare(point.y, y) != 0) return false;
        if (Double.compare(point.x, x) != 0) return false;
        return Double.compare(point.z, z) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public boolean hasZ() {
        return Double.isNaN(z) == false;
    }

    @Override
    public String toString() {
        return WellKnownText.INSTANCE.toWKT(this);
    }

}
