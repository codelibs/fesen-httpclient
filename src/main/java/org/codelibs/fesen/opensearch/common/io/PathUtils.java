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

package org.codelibs.fesen.opensearch.common.io;

import org.codelibs.fesen.opensearch.common.SuppressForbidden;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities for creating a Path from names,
 * or accessing the default FileSystem.
 * <p>
 * This class allows the default filesystem to
 * be changed during tests.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "accesses the default filesystem by design")
// TODO: can we move this to the .env package and make it package-private?
public final class PathUtils {
    /** no instantiation */
    private PathUtils() {}

    /** the actual JDK default */
    static final FileSystem ACTUAL_DEFAULT = FileSystems.getDefault();

    /** can be changed by tests */
    static volatile FileSystem DEFAULT = ACTUAL_DEFAULT;

    /**
     * Returns a {@code Path} from name components.
     * <p>
     * This works just like {@code Paths.get()}.
     * Remember: just like {@code Paths.get()} this is NOT A STRING CONCATENATION
     * UTILITY FUNCTION.
     * <p>
     * Remember: this should almost never be used. Usually resolve
     * a path against an existing one!
     *
     * @param first the first
     * @param more the more
     * @return the value
     */
    public static Path get(String first, String... more) {
        return DEFAULT.getPath(first, more);
    }
}
