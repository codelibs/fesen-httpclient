/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/* @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.common.util.io;

import org.codelibs.fesen.opensearch.common.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utilities for common I/O methods. Borrowed heavily from Lucene (org.apache.lucene.util.IOUtils).
 *
 * @opensearch.internal
 */
public final class IOUtils {

    /**
     * UTF-8 charset string.
     * <p>Where possible, use {@link StandardCharsets#UTF_8} instead,
     * as using the String constant may slow things down.
     * @see StandardCharsets#UTF_8
     */
    public static final String UTF_8 = StandardCharsets.UTF_8.name();

    private IOUtils() {
        // Static utils methods
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method either throws the first exception it hit
     * while closing with other exceptions added as suppressed, or completes normally if there were
     * no exceptions.
     *
     * @param objects objects to close
     */
    public static void close(final Closeable... objects) throws IOException {
        close(null, Arrays.asList(objects));
    }

    /**
     * @see #close(Closeable...)
     */
    public static void close(@Nullable Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method adds any exceptions as suppressed to the
     * original exception, or throws the first exception it hit if {@code Exception} is null. If
     * no exceptions are encountered and the passed in exception is null, it completes normally.
     *
     * @param objects objects to close
     */
    public static void close(final Exception e, final Closeable... objects) throws IOException {
        close(e, Arrays.asList(objects));
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method either throws the first exception it hit
     * while closing with other exceptions added as suppressed, or completes normally if there were
     * no exceptions.
     *
     * @param objects objects to close
     */
    public static void close(final Iterable<? extends Closeable> objects) throws IOException {
        close(null, objects);
    }

    /**
     * Closes all given {@link Closeable}s. If a non-null exception is passed in, or closing a
     * stream causes an exception, throws the exception with other {@link RuntimeException} or
     * {@link IOException} exceptions added as suppressed.
     *
     * @param ex existing Exception to add exceptions occurring during close to
     * @param objects objects to close
     *
     * @see #close(Closeable...)
     */
    public static void close(final Exception ex, final Iterable<? extends Closeable> objects) throws IOException {
        Exception firstException = ex;
        for (final Closeable object : objects) {
            try {
                close(object);
            } catch (final IOException | RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            if (firstException instanceof IOException ioe) {
                throw ioe;
            } else {
                // since we only assigned an IOException or a RuntimeException to ex above, in this case ex must be a RuntimeException
                throw (RuntimeException) firstException;
            }
        }
    }

    /**
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(final Closeable closeable) {
        // noinspection EmptyCatchBlock
        try {
            close(closeable);
        } catch (final IOException | RuntimeException e) {}
    }

    // TODO: replace with constants class if needed (cf. org.apache.lucene.util.Constants)
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");
    public static final boolean LINUX = System.getProperty("os.name").startsWith("Linux");
    public static final boolean MAC_OS_X = System.getProperty("os.name").startsWith("Mac OS X");
}
