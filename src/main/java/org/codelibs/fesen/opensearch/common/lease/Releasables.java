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

package org.codelibs.fesen.opensearch.common.lease;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility methods to work with {@link Releasable}s.
 *
 * @opensearch.internal
 */
public enum Releasables {
    ;

    private static void close(Iterable<? extends Releasable> releasables, boolean ignoreException) {
        try {
            // this does the right thing with respect to add suppressed and not wrapping errors etc.
            IOUtils.close(releasables);
        } catch (IOException e) {
            if (ignoreException == false) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Iterable<? extends Releasable> releasables) {
        close(releasables, false);
    }

    /** Release the provided {@link Releasable}. */
    public static void close(@Nullable Releasable releasable) {
        try {
            IOUtils.close(releasable);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Releasable... releasables) {
        close(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Iterable<? extends Releasable> releasables) {
        close(releasables, true);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Releasable... releasables) {
        closeWhileHandlingException(Arrays.asList(releasables));
    }
}
