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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.io.PathUtils;

/**
 * Checks whether the operating system setting {@code vm.max_map_count} meets
 * the minimum value required to run OpenSearch/Elasticsearch.
 */
public class MaxMapCountCheck {
    /** The minimum required value of {@code vm.max_map_count}. */
    public static final long LIMIT = 1 << 18;

    private static final Logger logger = LogManager.getLogger(MaxMapCountCheck.class);

    private MaxMapCountCheck() {
        // utility class
    }

    /**
     * Validates that {@code vm.max_map_count} meets the required minimum value.
     *
     * @return {@code true} if the value meets {@link #LIMIT} or cannot be determined
     */
    public static boolean validate() {
        final long maxMapCount = getMaxMapCount();
        return maxMapCount == -1 || maxMapCount >= LIMIT;
    }

    /**
     * Reads the value of {@code vm.max_map_count} from {@code /proc/sys/vm/max_map_count}.
     *
     * @return the value, or {@code -1} if it cannot be read or parsed
     */
    protected static long getMaxMapCount() {
        final Path path = PathUtils.get("/proc/sys/vm/max_map_count");
        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            final String rawProcSysVmMaxMapCount = bufferedReader.readLine();
            if (rawProcSysVmMaxMapCount != null) {
                try {
                    return Long.parseLong(rawProcSysVmMaxMapCount);
                } catch (final NumberFormatException e) {
                    logger.debug(() -> new ParameterizedMessage("unable to parse vm.max_map_count [{}]", rawProcSysVmMaxMapCount), e);
                }
            }
        } catch (final IOException e) {
            logger.debug(() -> new ParameterizedMessage("I/O exception while trying to read [{}]", path), e);
        }
        return -1;
    }

}
