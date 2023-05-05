/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Locale;

import org.junit.jupiter.api.Test;

class MaxMapCountCheckTest {
    @Test
    void test_validate() throws Exception {
        long maxMapCount = MaxMapCountCheck.getMaxMapCount();
        if (maxMapCount == -1) {
            assertTrue(MaxMapCountCheck.validate());
        } else if (maxMapCount < MaxMapCountCheck.LIMIT) {
            assertTrue(!MaxMapCountCheck.validate());
        } else {
            assertTrue(MaxMapCountCheck.validate());
        }

        String message = String.format(Locale.ROOT, //
                "max virtual memory areas vm.max_map_count for [%s] might be too low, increase to at least [%d]. ", //
                "http://localhost:9200", //
                MaxMapCountCheck.LIMIT);
        assertEquals(
                "max virtual memory areas vm.max_map_count for [http://localhost:9200] might be too low, increase to at least [262144]. ",
                message);
    }
}
