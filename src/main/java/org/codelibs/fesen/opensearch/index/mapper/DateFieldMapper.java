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
package org.codelibs.fesen.opensearch.index.mapper;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.time.DateFormatter;
import org.codelibs.fesen.opensearch.common.time.DateUtils;
import org.codelibs.fesen.opensearch.common.util.FeatureFlags;

import java.time.Instant;

/**
 * The client-side remnant of the date field mapper: the date formats and the millisecond /
 * nanosecond resolution a client needs to render and parse date doc values. Indexing and querying
 * dates is a node-side concern and is not carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class DateFieldMapper {

    /** The {@code date} field type name. */
    public static final String CONTENT_TYPE = "date";
    /** The {@code date_nanos} field type name. */
    public static final String DATE_NANOS_CONTENT_TYPE = "date_nanos";

    /** The date format used before the datetime-formatter-caching feature flag existed. */
    @Deprecated
    public static final DateFormatter LEGACY_DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern(
        "strict_date_optional_time||epoch_millis"
    );

    /** The default date format. */
    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern(
        "strict_date_time_no_millis||strict_date_optional_time||epoch_millis",
        "strict_date_optional_time"
    );

    private DateFieldMapper() {
    }

    /**
     * Returns the date format a field without an explicit {@code format} uses.
     *
     * @return the default date formatter
     */
    public static DateFormatter getDefaultDateTimeFormatter() {
        return FeatureFlags.isEnabled(FeatureFlags.DATETIME_FORMATTER_CACHING_SETTING)
            ? DEFAULT_DATE_TIME_FORMATTER
            : LEGACY_DEFAULT_DATE_TIME_FORMATTER;
    }

    /**
     * Resolution of the date time.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Resolution {
        MILLISECONDS(CONTENT_TYPE) {
            @Override
            public long convert(Instant instant) {
                return clampToValidRange(instant).toEpochMilli();
            }

            @Override
            public Instant toInstant(long value) {
                return Instant.ofEpochMilli(value);
            }

            @Override
            public Instant clampToValidRange(Instant instant) {
                return DateUtils.clampToMillisRange(instant);
            }
        },
        NANOSECONDS(DATE_NANOS_CONTENT_TYPE) {
            @Override
            public long convert(Instant instant) {
                return DateUtils.toLong(instant);
            }

            @Override
            public Instant toInstant(long value) {
                return DateUtils.toInstant(value);
            }

            @Override
            public Instant clampToValidRange(Instant instant) {
                return DateUtils.clampToNanosRange(instant);
            }
        };

        private final String type;

        Resolution(String type) {
            this.type = type;
        }

        /**
         * Returns the field type name this resolution belongs to.
         *
         * @return {@code date} or {@code date_nanos}
         */
        public String type() {
            return type;
        }

        /**
         * Convert an {@linkplain Instant} into a long value in this resolution.
         *
         * @param instant the instant to convert
         * @return the instant in this resolution
         */
        public abstract long convert(Instant instant);

        /**
         * Convert a long value in this resolution into an instant.
         *
         * @param value the value to convert
         * @return the instant the value represents
         */
        public abstract Instant toInstant(long value);

        /**
         * Return the instant that this resolution can represent that is closest to the provided instant.
         *
         * @param instant the instant to clamp
         * @return the clamped instant
         */
        public abstract Instant clampToValidRange(Instant instant);

        /**
         * Returns the resolution with the given ordinal, as written on the wire.
         *
         * @param ord the ordinal
         * @return the resolution
         */
        public static Resolution ofOrdinal(int ord) {
            for (Resolution resolution : values()) {
                if (ord == resolution.ordinal()) {
                    return resolution;
                }
            }
            throw new IllegalArgumentException("unknown resolution ordinal [" + ord + "]");
        }
    }
}
