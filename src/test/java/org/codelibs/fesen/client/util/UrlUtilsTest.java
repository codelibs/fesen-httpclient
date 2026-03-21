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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class UrlUtilsTest {

    @Test
    void test_encode_withSimpleString() {
        assertEquals("hello", UrlUtils.encode("hello"));
    }

    @Test
    void test_encode_withNull() {
        assertNull(UrlUtils.encode(null));
    }

    @Test
    void test_encode_withSpecialCharacters() {
        assertEquals("hello+world", UrlUtils.encode("hello world"));
        assertEquals("%2F", UrlUtils.encode("/"));
        assertEquals("%3A", UrlUtils.encode(":"));
        assertEquals("%23", UrlUtils.encode("#"));
        assertEquals("%3F", UrlUtils.encode("?"));
        assertEquals("%26", UrlUtils.encode("&"));
        assertEquals("%3D", UrlUtils.encode("="));
    }

    @Test
    void test_encode_withUnicodeCharacters() {
        final String encoded = UrlUtils.encode("\u65E5\u672C\u8A9E");
        assertEquals("%E6%97%A5%E6%9C%AC%E8%AA%9E", encoded);
    }

    @Test
    void test_encode_withEmptyString() {
        assertEquals("", UrlUtils.encode(""));
    }

    @Test
    void test_joinAndEncode_withNull() {
        assertNull(UrlUtils.joinAndEncode(",", (CharSequence[]) null));
    }

    @Test
    void test_joinAndEncode_withSingleElement() {
        assertEquals("index1", UrlUtils.joinAndEncode(",", "index1"));
    }

    @Test
    void test_joinAndEncode_withMultipleElements() {
        assertEquals("index1,index2,index3", UrlUtils.joinAndEncode(",", "index1", "index2", "index3"));
    }

    @Test
    void test_joinAndEncode_withSpecialCharactersInElements() {
        assertEquals("my+index,your+index", UrlUtils.joinAndEncode(",", "my index", "your index"));
    }

    @Test
    void test_joinAndEncode_withNullElement() {
        final String result = UrlUtils.joinAndEncode(",", "index1", null, "index3");
        assertEquals("index1,null,index3", result);
    }

    @Test
    void test_joinAndEncode_withEmptyArray() {
        assertEquals("", UrlUtils.joinAndEncode(",", new CharSequence[0]));
    }

    @Test
    void test_joinAndEncode_withDifferentDelimiter() {
        assertEquals("a/b/c", UrlUtils.joinAndEncode("/", "a", "b", "c"));
    }
}
