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

package org.codelibs.fesen.opensearch.core.util;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.codelibs.fesen.opensearch.common.SuppressForbidden;
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.StreamSupport;

/**
 * OpenSearch utils to work with {@link java.nio.file.Path}
 *
 * @opensearch.internal
 */
public final class FileSystemUtils {

    private FileSystemUtils() {} // only static methods

    /**
     * Returns an InputStream the given url if the url has a protocol of 'file' or 'jar', no host, and no port.
     *
     * @param url the URL
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    @SuppressForbidden(reason = "Will only open url streams for local files")
    public static InputStream openFileURLStream(URL url) throws IOException {
        String protocol = url.getProtocol();
        if ("file".equals(protocol) == false && "jar".equals(protocol) == false) {
            throw new IllegalArgumentException("Invalid protocol [" + protocol + "], must be [file] or [jar]");
        }
        if (url.getHost().isEmpty() == false) {
            throw new IllegalArgumentException("URL cannot have host. Found: [" + url.getHost() + ']');
        }
        if (url.getPort() != -1) {
            throw new IllegalArgumentException("URL cannot have port. Found: [" + url.getPort() + ']');
        }
        return url.openStream();
    }

}
