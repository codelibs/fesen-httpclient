/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.time;

import java.text.Format;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.Locale;

/**
* Wrapper class for DateTimeFormatter{@link java.time.format.DateTimeFormatter}
* to allow for custom implementations for datetime parsing/formatting
 */
class OpenSearchDateTimeFormatter implements OpenSearchDateTimePrinter {
    private final DateTimeFormatter formatter;

    public OpenSearchDateTimeFormatter(String pattern) {
        this.formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
    }

    public OpenSearchDateTimeFormatter(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    public OpenSearchDateTimeFormatter withLocale(Locale locale) {
        return new OpenSearchDateTimeFormatter(getFormatter().withLocale(locale));
    }

    public OpenSearchDateTimeFormatter withZone(ZoneId zoneId) {
        return new OpenSearchDateTimeFormatter(getFormatter().withZone(zoneId));
    }

    public String format(TemporalAccessor temporal) {
        return this.getFormatter().format(temporal);
    }

    public ZoneId getZone() {
        return this.getFormatter().getZone();
    }

    public Locale getLocale() {
        return this.getFormatter().getLocale();
    }

    public TemporalAccessor parse(String input) {
        return formatter.parse(input);
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    public Object parseObject(String text, ParsePosition pos) {
        return getFormatter().toFormat().parseObject(text, pos);
    }
}
