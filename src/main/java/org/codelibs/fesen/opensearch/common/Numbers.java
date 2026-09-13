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

package org.codelibs.fesen.opensearch.common;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A set of utilities for numbers.
 *
 * @opensearch.internal
 */
public final class Numbers {
    public static final BigInteger MAX_UNSIGNED_LONG_VALUE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    public static final BigInteger MIN_UNSIGNED_LONG_VALUE = BigInteger.ZERO;

    public static final long MIN_UNSIGNED_LONG_VALUE_AS_LONG = MIN_UNSIGNED_LONG_VALUE.longValue();
    public static final long MAX_UNSIGNED_LONG_VALUE_AS_LONG = MAX_UNSIGNED_LONG_VALUE.longValue();

    private static final BigInteger MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    private Numbers() {}

    /** Returns true if value is neither NaN nor infinite. */
    public static boolean isValidDouble(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return false;
        }
        return true;
    }

    // weak bounds on the BigDecimal representation to allow for coercion
    private static BigDecimal BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_GREATER_THAN_USIGNED_LONG_MAX_VALUE = new BigDecimal(MAX_UNSIGNED_LONG_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_USIGNED_LONG_MIN_VALUE = new BigDecimal(MIN_UNSIGNED_LONG_VALUE).subtract(
        BigDecimal.ONE
    );

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    public static long toLong(String stringValue, boolean coerce) {
        try {
            return Long.parseLong(stringValue);
        } catch (NumberFormatException e) {
            // we will try again with BigDecimal
        }

        final BigInteger bigIntegerValue;
        try {
            BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE) >= 0
                || bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(MAX_LONG_VALUE) > 0 || bigIntegerValue.compareTo(MIN_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
        }

        return bigIntegerValue.longValue();
    }

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    public static BigInteger toUnsignedLong(String stringValue, boolean coerce) {
        final BigInteger bigIntegerValue;
        try {
            BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_USIGNED_LONG_MAX_VALUE) >= 0
                || bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_USIGNED_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for an unsigned long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(MAX_UNSIGNED_LONG_VALUE) > 0 || bigIntegerValue.compareTo(MIN_UNSIGNED_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for an unsigned long");
        }

        return bigIntegerValue;
    }

    /**
     * Return a BigInteger equal to the unsigned value of the
     * argument.
     */
    public static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L) return BigInteger.valueOf(i);
        else {
            int upper = (int) (i >>> 32);
            int lower = (int) i;

            // return (upper << 32) + lower
            return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
        }
    }

    /**
     * Convert unsigned long to double value (see please Guava's com.google.common.primitives.UnsignedLong),
     * this is faster then going through {@link #toUnsignedBigInteger(long)} conversion.
     */
    public static double unsignedLongToDouble(long value) {
        if (value >= 0) {
            return (double) value;
        }

        // The top bit is set, which means that the double value is going to come from the top 53 bits.
        // So we can ignore the bottom 11, except for rounding. We can unsigned-shift right 1, aka
        // unsigned-divide by 2, and convert that. Then we'll get exactly half of the desired double
        // value. But in the specific case where the bottom two bits of the original number are 01, we
        // want to replace that with 1 in the shifted value for correct rounding.
        return (double) ((value >>> 1) | (value & 1)) * 2.0;
    }

    /**
     * Return the strictly greater next power of two for the given value.
     * For zero and negative numbers, it returns 1.
     */
    public static long nextPowerOfTwo(long value) {
        return 1L << (Long.SIZE - Long.numberOfLeadingZeros(value));
    }
}
