/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.semver.expr;

import org.codelibs.fesen.opensearch.Version;

import java.util.Objects;

/**
 * Expression to evaluate version compatibility within a specified range with configurable bounds.
 */
public class Range implements Expression {
    private final Version lowerBound;
    private final Version upperBound;
    private final boolean includeLower;
    private final boolean includeUpper;

    /**
     * Creates a new Range.
     */
    public Range() {
        this.lowerBound = Version.fromString("0.0.0");  // Minimum version
        this.upperBound = Version.fromString("99.99.99"); // Maximum version
        this.includeLower = true;  // Default to inclusive bounds
        this.includeUpper = true;
    }

    /**
     * Creates a new Range.
     *
     * @param lowerBound the lower bound
     * @param upperBound the upper bound
     * @param includeLower the include lower
     * @param includeUpper the include upper
     */
    public Range(Version lowerBound, Version upperBound, boolean includeLower, boolean includeUpper) {
        if (lowerBound == null) {
            throw new IllegalArgumentException("Lower bound cannot be null");
        }
        if (upperBound == null) {
            throw new IllegalArgumentException("Upper bound cannot be null");
        }
        if (lowerBound.after(upperBound)) {
            throw new IllegalArgumentException("Lower bound must be less than or equal to upper bound");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    public boolean evaluate(final Version rangeVersion, final Version versionToEvaluate) {

        boolean satisfiesLower = includeLower ? versionToEvaluate.onOrAfter(lowerBound) : versionToEvaluate.after(lowerBound);

        boolean satisfiesUpper = includeUpper ? versionToEvaluate.onOrBefore(upperBound) : versionToEvaluate.before(upperBound);

        return satisfiesLower && satisfiesUpper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return includeLower == range.includeLower
            && includeUpper == range.includeUpper
            && Objects.equals(lowerBound, range.lowerBound)
            && Objects.equals(upperBound, range.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound, includeLower, includeUpper);
    }

    /**
     * Returns the include lower flag.
     *
     * @return the include lower flag
     */
    public boolean isIncludeLower() {
        return includeLower;
    }

    /**
     * Returns the include upper flag.
     *
     * @return the include upper flag
     */
    public boolean isIncludeUpper() {
        return includeUpper;
    }

    /**
     * Returns the lower bound.
     *
     * @return the lower bound
     */
    public Version getLowerBound() {
        return lowerBound;
    }

    /**
     * Returns the upper bound.
     *
     * @return the upper bound
     */
    public Version getUpperBound() {
        return upperBound;
    }

}
