/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility methods for URL-encoding values used in request paths and parameters.
 */
public final class UrlUtils {

    private static final Logger logger = LogManager.getLogger(UrlUtils.class);

    private UrlUtils() {
        // nothing
    }

    /**
     * URL-encodes each element and joins them with the given delimiter.
     *
     * @param delimiter the delimiter placed between the encoded elements
     * @param elements the elements to encode and join
     * @return the joined string, or {@code null} if {@code elements} is {@code null}
     */
    public static String joinAndEncode(final CharSequence delimiter, final CharSequence... elements) {
        if (elements == null) {
            return null;
        }
        return Arrays.stream(elements).map(UrlUtils::encode).collect(Collectors.joining(delimiter));
    }

    /**
     * URL-encodes the given value using UTF-8.
     *
     * @param element the value to encode
     * @return the encoded value, or {@code null} if {@code element} is {@code null}
     */
    public static String encode(final CharSequence element) {
        if (element == null) {
            return null;
        }
        try {
            return URLEncoder.encode(element.toString(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Invalid encoding.", e);
            }
            return element.toString();
        }
    }
}
