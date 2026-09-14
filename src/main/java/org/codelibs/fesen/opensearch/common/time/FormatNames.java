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

package org.codelibs.fesen.opensearch.common.time;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date format names.
 *
 * @opensearch.internal
 */
public enum FormatNames {
    /**
     * The ISO8601 value.
     */
    ISO8601(null, "iso8601"),
    /**
     * The RFC3339_LENIENT value.
     */
    RFC3339_LENIENT(null, "rfc3339_lenient"),
    /**
     * The BASIC_DATE value.
     */
    BASIC_DATE("basicDate", "basic_date"),
    /**
     * The BASIC_DATE_TIME value.
     */
    BASIC_DATE_TIME("basicDateTime", "basic_date_time"),
    /**
     * The BASIC_DATE_TIME_NO_MILLIS value.
     */
    BASIC_DATE_TIME_NO_MILLIS("basicDateTimeNoMillis", "basic_date_time_no_millis"),
    /**
     * The BASIC_ORDINAL_DATE value.
     */
    BASIC_ORDINAL_DATE("basicOrdinalDate", "basic_ordinal_date"),
    /**
     * The BASIC_ORDINAL_DATE_TIME value.
     */
    BASIC_ORDINAL_DATE_TIME("basicOrdinalDateTime", "basic_ordinal_date_time"),
    /**
     * The BASIC_ORDINAL_DATE_TIME_NO_MILLIS value.
     */
    BASIC_ORDINAL_DATE_TIME_NO_MILLIS("basicOrdinalDateTimeNoMillis", "basic_ordinal_date_time_no_millis"),
    /**
     * The BASIC_TIME value.
     */
    BASIC_TIME("basicTime", "basic_time"),
    /**
     * The BASIC_TIME_NO_MILLIS value.
     */
    BASIC_TIME_NO_MILLIS("basicTimeNoMillis", "basic_time_no_millis"),
    /**
     * The BASIC_T_TIME value.
     */
    BASIC_T_TIME("basicTTime", "basic_t_time"),
    /**
     * The BASIC_T_TIME_NO_MILLIS value.
     */
    BASIC_T_TIME_NO_MILLIS("basicTTimeNoMillis", "basic_t_time_no_millis"),
    /**
     * The BASIC_WEEK_DATE value.
     */
    BASIC_WEEK_DATE("basicWeekDate", "basic_week_date"),
    /**
     * The BASIC_WEEK_DATE_TIME value.
     */
    BASIC_WEEK_DATE_TIME("basicWeekDateTime", "basic_week_date_time"),
    /**
     * The BASIC_WEEK_DATE_TIME_NO_MILLIS value.
     */
    BASIC_WEEK_DATE_TIME_NO_MILLIS("basicWeekDateTimeNoMillis", "basic_week_date_time_no_millis"),
    /**
     * The DATE value.
     */
    DATE(null, "date"),
    /**
     * The DATE_HOUR value.
     */
    DATE_HOUR("dateHour", "date_hour"),
    /**
     * The DATE_HOUR_MINUTE value.
     */
    DATE_HOUR_MINUTE("dateHourMinute", "date_hour_minute"),
    /**
     * The DATE_HOUR_MINUTE_SECOND value.
     */
    DATE_HOUR_MINUTE_SECOND("dateHourMinuteSecond", "date_hour_minute_second"),
    /**
     * The DATE_HOUR_MINUTE_SECOND_FRACTION value.
     */
    DATE_HOUR_MINUTE_SECOND_FRACTION("dateHourMinuteSecondFraction", "date_hour_minute_second_fraction"),
    /**
     * The DATE_HOUR_MINUTE_SECOND_MILLIS value.
     */
    DATE_HOUR_MINUTE_SECOND_MILLIS("dateHourMinuteSecondMillis", "date_hour_minute_second_millis"),
    /**
     * The DATE_OPTIONAL_TIME value.
     */
    DATE_OPTIONAL_TIME("dateOptionalTime", "date_optional_time"),
    /**
     * The DATE_TIME value.
     */
    DATE_TIME("dateTime", "date_time"),
    /**
     * The DATE_TIME_NO_MILLIS value.
     */
    DATE_TIME_NO_MILLIS("dateTimeNoMillis", "date_time_no_millis"),
    /**
     * The HOUR value.
     */
    HOUR(null, "hour"),
    /**
     * The HOUR_MINUTE value.
     */
    HOUR_MINUTE("hourMinute", "hour_minute"),
    /**
     * The HOUR_MINUTE_SECOND value.
     */
    HOUR_MINUTE_SECOND("hourMinuteSecond", "hour_minute_second"),
    /**
     * The HOUR_MINUTE_SECOND_FRACTION value.
     */
    HOUR_MINUTE_SECOND_FRACTION("hourMinuteSecondFraction", "hour_minute_second_fraction"),
    /**
     * The HOUR_MINUTE_SECOND_MILLIS value.
     */
    HOUR_MINUTE_SECOND_MILLIS("hourMinuteSecondMillis", "hour_minute_second_millis"),
    /**
     * The ORDINAL_DATE value.
     */
    ORDINAL_DATE("ordinalDate", "ordinal_date"),
    /**
     * The ORDINAL_DATE_TIME value.
     */
    ORDINAL_DATE_TIME("ordinalDateTime", "ordinal_date_time"),
    /**
     * The ORDINAL_DATE_TIME_NO_MILLIS value.
     */
    ORDINAL_DATE_TIME_NO_MILLIS("ordinalDateTimeNoMillis", "ordinal_date_time_no_millis"),
    /**
     * The TIME value.
     */
    TIME(null, "time"),
    /**
     * The TIME_NO_MILLIS value.
     */
    TIME_NO_MILLIS("timeNoMillis", "time_no_millis"),
    /**
     * The T_TIME value.
     */
    T_TIME("tTime", "t_time"),
    /**
     * The T_TIME_NO_MILLIS value.
     */
    T_TIME_NO_MILLIS("tTimeNoMillis", "t_time_no_millis"),
    /**
     * The WEEK_DATE value.
     */
    WEEK_DATE("weekDate", "week_date"),
    /**
     * The WEEK_DATE_TIME value.
     */
    WEEK_DATE_TIME("weekDateTime", "week_date_time"),
    /**
     * The WEEK_DATE_TIME_NO_MILLIS value.
     */
    WEEK_DATE_TIME_NO_MILLIS("weekDateTimeNoMillis", "week_date_time_no_millis"),
    /**
     * The WEEK_YEAR value.
     */
    WEEK_YEAR(null, "week_year"),
    /**
     * The WEEKYEAR value.
     */
    WEEKYEAR(null, "weekyear"),
    /**
     * The WEEK_YEAR_WEEK value.
     */
    WEEK_YEAR_WEEK("weekyearWeek", "weekyear_week"),
    /**
     * The WEEKYEAR_WEEK_DAY value.
     */
    WEEKYEAR_WEEK_DAY("weekyearWeekDay", "weekyear_week_day"),
    /**
     * The YEAR value.
     */
    YEAR(null, "year"),
    /**
     * The YEAR_MONTH value.
     */
    YEAR_MONTH("yearMonth", "year_month"),
    /**
     * The YEAR_MONTH_DAY value.
     */
    YEAR_MONTH_DAY("yearMonthDay", "year_month_day"),
    /**
     * The EPOCH_SECOND value.
     */
    EPOCH_SECOND(null, "epoch_second"),
    /**
     * The EPOCH_MILLIS value.
     */
    EPOCH_MILLIS(null, "epoch_millis"),
    /**
     * The EPOCH_MICROS value.
     */
    EPOCH_MICROS(null, "epoch_micros"),
    // strict date formats here, must be at least 4 digits for year and two for months and two for day"
    /**
     * The STRICT_BASIC_WEEK_DATE value.
     */
    STRICT_BASIC_WEEK_DATE("strictBasicWeekDate", "strict_basic_week_date"),
    /**
     * The STRICT_BASIC_WEEK_DATE_TIME value.
     */
    STRICT_BASIC_WEEK_DATE_TIME("strictBasicWeekDateTime", "strict_basic_week_date_time"),
    /**
     * The STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS value.
     */
    STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS("strictBasicWeekDateTimeNoMillis", "strict_basic_week_date_time_no_millis"),
    /**
     * The STRICT_DATE value.
     */
    STRICT_DATE("strictDate", "strict_date"),
    /**
     * The STRICT_DATE_HOUR value.
     */
    STRICT_DATE_HOUR("strictDateHour", "strict_date_hour"),
    /**
     * The STRICT_DATE_HOUR_MINUTE value.
     */
    STRICT_DATE_HOUR_MINUTE("strictDateHourMinute", "strict_date_hour_minute"),
    /**
     * The STRICT_DATE_HOUR_MINUTE_SECOND value.
     */
    STRICT_DATE_HOUR_MINUTE_SECOND("strictDateHourMinuteSecond", "strict_date_hour_minute_second"),
    /**
     * The STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION value.
     */
    STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION("strictDateHourMinuteSecondFraction", "strict_date_hour_minute_second_fraction"),
    /**
     * The STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS value.
     */
    STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS("strictDateHourMinuteSecondMillis", "strict_date_hour_minute_second_millis"),
    /**
     * The STRICT_DATE_OPTIONAL_TIME value.
     */
    STRICT_DATE_OPTIONAL_TIME("strictDateOptionalTime", "strict_date_optional_time"),
    /**
     * The STRICT_DATE_OPTIONAL_TIME_NANOS value.
     */
    STRICT_DATE_OPTIONAL_TIME_NANOS("strictDateOptionalTimeNanos", "strict_date_optional_time_nanos"),
    /**
     * The STRICT_DATE_TIME value.
     */
    STRICT_DATE_TIME("strictDateTime", "strict_date_time"),
    /**
     * The STRICT_DATE_TIME_NO_MILLIS value.
     */
    STRICT_DATE_TIME_NO_MILLIS("strictDateTimeNoMillis", "strict_date_time_no_millis"),
    /**
     * The STRICT_HOUR value.
     */
    STRICT_HOUR("strictHour", "strict_hour"),
    /**
     * The STRICT_HOUR_MINUTE value.
     */
    STRICT_HOUR_MINUTE("strictHourMinute", "strict_hour_minute"),
    /**
     * The STRICT_HOUR_MINUTE_SECOND value.
     */
    STRICT_HOUR_MINUTE_SECOND("strictHourMinuteSecond", "strict_hour_minute_second"),
    /**
     * The STRICT_HOUR_MINUTE_SECOND_FRACTION value.
     */
    STRICT_HOUR_MINUTE_SECOND_FRACTION("strictHourMinuteSecondFraction", "strict_hour_minute_second_fraction"),
    /**
     * The STRICT_HOUR_MINUTE_SECOND_MILLIS value.
     */
    STRICT_HOUR_MINUTE_SECOND_MILLIS("strictHourMinuteSecondMillis", "strict_hour_minute_second_millis"),
    /**
     * The STRICT_ORDINAL_DATE value.
     */
    STRICT_ORDINAL_DATE("strictOrdinalDate", "strict_ordinal_date"),
    /**
     * The STRICT_ORDINAL_DATE_TIME value.
     */
    STRICT_ORDINAL_DATE_TIME("strictOrdinalDateTime", "strict_ordinal_date_time"),
    /**
     * The STRICT_ORDINAL_DATE_TIME_NO_MILLIS value.
     */
    STRICT_ORDINAL_DATE_TIME_NO_MILLIS("strictOrdinalDateTimeNoMillis", "strict_ordinal_date_time_no_millis"),
    /**
     * The STRICT_TIME value.
     */
    STRICT_TIME("strictTime", "strict_time"),
    /**
     * The STRICT_TIME_NO_MILLIS value.
     */
    STRICT_TIME_NO_MILLIS("strictTimeNoMillis", "strict_time_no_millis"),
    /**
     * The STRICT_T_TIME value.
     */
    STRICT_T_TIME("strictTTime", "strict_t_time"),
    /**
     * The STRICT_T_TIME_NO_MILLIS value.
     */
    STRICT_T_TIME_NO_MILLIS("strictTTimeNoMillis", "strict_t_time_no_millis"),
    /**
     * The STRICT_WEEK_DATE value.
     */
    STRICT_WEEK_DATE("strictWeekDate", "strict_week_date"),
    /**
     * The STRICT_WEEK_DATE_TIME value.
     */
    STRICT_WEEK_DATE_TIME("strictWeekDateTime", "strict_week_date_time"),
    /**
     * The STRICT_WEEK_DATE_TIME_NO_MILLIS value.
     */
    STRICT_WEEK_DATE_TIME_NO_MILLIS("strictWeekDateTimeNoMillis", "strict_week_date_time_no_millis"),
    /**
     * The STRICT_WEEKYEAR value.
     */
    STRICT_WEEKYEAR("strictWeekyear", "strict_weekyear"),
    /**
     * The STRICT_WEEKYEAR_WEEK value.
     */
    STRICT_WEEKYEAR_WEEK("strictWeekyearWeek", "strict_weekyear_week"),
    /**
     * The STRICT_WEEKYEAR_WEEK_DAY value.
     */
    STRICT_WEEKYEAR_WEEK_DAY("strictWeekyearWeekDay", "strict_weekyear_week_day"),
    /**
     * The STRICT_YEAR value.
     */
    STRICT_YEAR("strictYear", "strict_year"),
    /**
     * The STRICT_YEAR_MONTH value.
     */
    STRICT_YEAR_MONTH("strictYearMonth", "strict_year_month"),
    /**
     * The STRICT_YEAR_MONTH_DAY value.
     */
    STRICT_YEAR_MONTH_DAY("strictYearMonthDay", "strict_year_month_day");

    private static final Set<String> ALL_NAMES = Arrays.stream(values())
        .flatMap(n -> Stream.of(n.snakeCaseName, n.camelCaseName))
        .collect(Collectors.toSet());
    private final String camelCaseName;
    private final String snakeCaseName;

    FormatNames(String camelCaseName, String snakeCaseName) {
        this.camelCaseName = camelCaseName;
        this.snakeCaseName = snakeCaseName;
    }

    /**
     * Returns the exist.
     *
     * @param format the format
     * @return the exist
     */
    public static boolean exist(String format) {
        return ALL_NAMES.contains(format);
    }

    /**
     * Returns the for name.
     *
     * @param format the format
     * @return the for name
     */
    public static FormatNames forName(String format) {
        for (FormatNames name : values()) {
            if (name.matches(format)) {
                return name;
            }
        }
        return null;
    }

    /**
     * Returns the matches.
     *
     * @param format the format
     * @return the matches
     */
    public boolean matches(String format) {
        return format.equals(camelCaseName) || format.equals(snakeCaseName);
    }

    /**
     * Returns the camel case flag.
     *
     * @param format the format
     * @return the camel case flag
     */
    public boolean isCamelCase(String format) {
        return format.equals(camelCaseName);
    }

    /**
     * Returns the snake case name.
     *
     * @return the snake case name
     */
    public String getSnakeCaseName() {
        return snakeCaseName;
    }

    /**
     * Returns the camel case name.
     *
     * @return the camel case name
     */
    public String getCamelCaseName() {
        return camelCaseName;
    }
}
