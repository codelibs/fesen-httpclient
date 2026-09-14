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

package org.codelibs.fesen.opensearch.common.logging;

import org.codelibs.fesen.opensearch.Build;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.opensearch.core.common.logging.LoggerMessageFormat;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a simplistic logger that adds warning messages to HTTP headers.
 * Use <code>HeaderWarning.addWarning(message,params)</code>. Message will be formatted according to RFC7234.
 * The result will be returned as HTTP response headers.
 *
 * @opensearch.internal
 */
public class HeaderWarning {
    /**
     * Creates a new HeaderWarning.
     */
    public HeaderWarning() {
    }

    /**
     * Regular expression to test if a string matches the RFC7234 specification for warning headers. This pattern assumes that the warn code
     * is always 299. Further, this pattern assumes that the warn agent represents a version of OpenSearch including the build hash.
     */
    public static final Pattern WARNING_HEADER_PATTERN = Pattern.compile("299 OpenSearch-" + // warn code
        "\\d+\\.\\d+\\.\\d+(?:-(?:alpha|beta|rc)\\d+)?(?:-SNAPSHOT)?-" + // warn agent
        "(?:[a-f0-9]{7}(?:[a-f0-9]{33})?|unknown) " + // warn agent
        "\"((?:\t| |!|[\\x23-\\x5B]|[\\x5D-\\x7E]|[\\x80-\\xFF]|\\\\|\\\\\")*)\"( " + // quoted warning value, captured
        // quoted RFC 1123 date format
        "\"" + // opening quote
        "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // weekday
        "\\d{2} " + // 2-digit day
        "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month
        "\\d{4} " + // 4-digit year
        "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
        "GMT" + // GMT
        "\")?"); // closing quote (optional, since an older version can still send a warn-date)
    /**
     * The WARNING_XCONTENT_LOCATION_PATTERN constant.
     */
    public static final Pattern WARNING_XCONTENT_LOCATION_PATTERN = Pattern.compile("^\\[.*?]\\[-?\\d+:-?\\d+] ");

    /*
     * RFC7234 specifies the warning format as warn-code <space> warn-agent <space> "warn-text" [<space> "warn-date"]. Here, warn-code is a
     * three-digit number with various standard warn codes specified. The warn code 299 is apt for our purposes as it represents a
     * miscellaneous persistent warning (can be presented to a human, or logged, and must not be removed by a cache). The warn-agent is an
     * arbitrary token; here we use the Elasticsearch version and build hash. The warn text must be quoted. The warn-date is an optional
     * quoted field that can be in a variety of specified date formats; here we use RFC 1123 format.
     */
    private static final String WARNING_PREFIX = String.format(
        Locale.ROOT,
        "299 OpenSearch-%s%s-%s",
        Version.CURRENT.toString(),
        Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "",
        Build.CURRENT.hash()
    );

    private static BitSet doesNotNeedEncoding;

    static {
        doesNotNeedEncoding = new BitSet(1 + 0xFF);
        doesNotNeedEncoding.set('\t');
        doesNotNeedEncoding.set(' ');
        doesNotNeedEncoding.set('!');
        doesNotNeedEncoding.set('\\');
        doesNotNeedEncoding.set('"');
        // we have to skip '%' which is 0x25 so that it is percent-encoded too
        for (int i = 0x23; i <= 0x24; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x26; i <= 0x5B; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x5D; i <= 0x7E; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x80; i <= 0xFF; i++) {
            doesNotNeedEncoding.set(i);
        }
        assert doesNotNeedEncoding.get('%') == false : doesNotNeedEncoding;
    }

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    /**
     * This is set once by the {@code Node} constructor, but it uses {@link CopyOnWriteArraySet} to ensure that tests can run in parallel.
     * <p>
     * Integration tests will create separate nodes within the same classloader, thus leading to a shared, {@code static} state.
     * In order for all tests to appropriately be handled, this must be able to remember <em>all</em> {@link ThreadContext}s that it is
     * given in a thread safe manner.
     * <p>
     * For actual usage, multiple nodes do not share the same JVM and therefore this will only be set once in practice.
     */
    static final CopyOnWriteArraySet<ThreadContext> THREAD_CONTEXT = new CopyOnWriteArraySet<>();

    /**
     * Returns the x opaque identifier.
     *
     * @return the x opaque identifier
     */
    public static String getXOpaqueId() {
        return THREAD_CONTEXT.stream()
            .filter(t -> t.getHeader(Task.X_OPAQUE_ID) != null)
            .findFirst()
            .map(t -> t.getHeader(Task.X_OPAQUE_ID))
            .orElse("");
    }
}
