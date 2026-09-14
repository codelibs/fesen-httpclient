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

package org.codelibs.fesen.opensearch.core.xcontent;

import org.codelibs.fesen.opensearch.common.CheckedFunction;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Interface for pull - parsing {@link XContent} see {@code XContentType} for supported types.
 * <p>
 * To obtain an instance of this class use the following pattern:
 *
 * <pre>
 *     MediaType mediaType = MediaTypeRegistry.JSON;
 *     XContentParser parser = mediaType.xContent().createParser(
 *          NamedXContentRegistry.EMPTY, ParserField."{\"key\" : \"value\"}");
 * </pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface XContentParser extends Closeable {

    /**
     * Supported serializable tokens
     *
     * @opensearch.internal
     */
    enum Token {
        /**
         * The START_OBJECT value.
         */
        START_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        /**
         * The END_OBJECT value.
         */
        END_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        /**
         * The START_ARRAY value.
         */
        START_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        /**
         * The END_ARRAY value.
         */
        END_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        /**
         * The FIELD_NAME value.
         */
        FIELD_NAME {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        /**
         * The VALUE_STRING value.
         */
        VALUE_STRING {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        /**
         * The VALUE_NUMBER value.
         */
        VALUE_NUMBER {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        /**
         * The VALUE_BOOLEAN value.
         */
        VALUE_BOOLEAN {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        // usually a binary value
        /**
         * The VALUE_EMBEDDED_OBJECT value.
         */
        VALUE_EMBEDDED_OBJECT {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        /**
         * The VALUE_NULL value.
         */
        VALUE_NULL {
            @Override
            public boolean isValue() {
                return false;
            }
        };

        /**
         * Returns the value flag.
         *
         * @return the value flag
         */
        public abstract boolean isValue();
    }

    /**
     * Supported numeric types
     *
     * @opensearch.internal
     */
    enum NumberType {
        /**
         * The INT value.
         */
        INT,
        /**
         * The BIG_INTEGER value.
         */
        BIG_INTEGER,
        /**
         * The LONG value.
         */
        LONG,
        /**
         * The FLOAT value.
         */
        FLOAT,
        /**
         * The DOUBLE value.
         */
        DOUBLE,
        /**
         * The big decimal.
         */
        BIG_DECIMAL
    }

    /**
     * Returns the content type.
     *
     * @return the content type
     */
    MediaType contentType();

    /**
     * Returns the next token.
     *
     * @return the next token
     * @throws IOException if an I/O error occurs
     */
    Token nextToken() throws IOException;

    /**
     * Skips the children.
     *
     * @throws IOException if an I/O error occurs
     */
    void skipChildren() throws IOException;

    /**
     * Returns the current token.
     *
     * @return the current token
     */
    Token currentToken();

    /**
     * Returns the current name.
     *
     * @return the current name
     * @throws IOException if an I/O error occurs
     */
    String currentName() throws IOException;

    /**
     * Returns the map.
     *
     * @return the map
     * @throws IOException if an I/O error occurs
     */
    Map<String, Object> map() throws IOException;

    /**
     * Returns the map ordered.
     *
     * @return the map ordered
     * @throws IOException if an I/O error occurs
     */
    Map<String, Object> mapOrdered() throws IOException;

    /**
     * Returns the map strings.
     *
     * @return the map strings
     * @throws IOException if an I/O error occurs
     */
    Map<String, String> mapStrings() throws IOException;

    /**
     * Returns an instance of {@link Map} holding parsed map.
     * Serves as a replacement for the "map", "mapOrdered" and "mapStrings" methods above.
     *
     * @param mapFactory factory for creating new {@link Map} objects
     * @param mapValueParser parser for parsing a single map value
     * @param <T> map value type
     * @return {@link Map} object
     * @throws IOException if an I/O error occurs
     */
    <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
        throws IOException;

    /**
     * Lists this instance.
     *
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    List<Object> list() throws IOException;

    /**
     * Lists the ordered map.
     *
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    List<Object> listOrderedMap() throws IOException;

    /**
     * Returns the text.
     *
     * @return the text
     * @throws IOException if an I/O error occurs
     */
    String text() throws IOException;

    /**
     * Returns the text or null.
     *
     * @return the text or null
     * @throws IOException if an I/O error occurs
     */
    String textOrNull() throws IOException;

    /**
     * Returns the char buffer or null.
     *
     * @return the char buffer or null
     * @throws IOException if an I/O error occurs
     */
    CharBuffer charBufferOrNull() throws IOException;

    /**
     * Returns a {@link CharBuffer} holding UTF-8 bytes.
     * This method should be used to read text only binary content should be read through {@link #binaryValue()}
     *
     * @return the char buffer
     * @throws IOException if an I/O error occurs
     */
    CharBuffer charBuffer() throws IOException;

    /**
     * Returns the object text.
     *
     * @return the object text
     * @throws IOException if an I/O error occurs
     */
    Object objectText() throws IOException;

    /**
     * Returns the object bytes.
     *
     * @return the object bytes
     * @throws IOException if an I/O error occurs
     */
    Object objectBytes() throws IOException;

    /**
     * Method that can be used to determine whether calling of textCharacters() would be the most efficient way to
     * access textual content for the event parser currently points to.
     * <p>
     * Default implementation simply returns false since only actual
     * implementation class has knowledge of its internal buffering
     * state.
     * <p>
     * This method shouldn't be used to check if the token contains text or not.
     *
     * @return the text characters flag
     */
    boolean hasTextCharacters();

    /**
     * Returns the text characters.
     *
     * @return the text characters
     * @throws IOException if an I/O error occurs
     */
    char[] textCharacters() throws IOException;

    /**
     * Returns the text length.
     *
     * @return the text length
     * @throws IOException if an I/O error occurs
     */
    int textLength() throws IOException;

    /**
     * Returns the text offset.
     *
     * @return the text offset
     * @throws IOException if an I/O error occurs
     */
    int textOffset() throws IOException;

    /**
     * Returns the number value.
     *
     * @return the number value
     * @throws IOException if an I/O error occurs
     */
    Number numberValue() throws IOException;

    /**
     * Returns the number type.
     *
     * @return the number type
     * @throws IOException if an I/O error occurs
     */
    NumberType numberType() throws IOException;

    /**
     * Returns the short value.
     *
     * @param coerce the coerce
     * @return the short value
     * @throws IOException if an I/O error occurs
     */
    short shortValue(boolean coerce) throws IOException;

    /**
     * Returns the int value.
     *
     * @param coerce the coerce
     * @return the int value
     * @throws IOException if an I/O error occurs
     */
    int intValue(boolean coerce) throws IOException;

    /**
     * Returns the long value.
     *
     * @param coerce the coerce
     * @return the long value
     * @throws IOException if an I/O error occurs
     */
    long longValue(boolean coerce) throws IOException;

    /**
     * Returns the float value.
     *
     * @param coerce the coerce
     * @return the float value
     * @throws IOException if an I/O error occurs
     */
    float floatValue(boolean coerce) throws IOException;

    /**
     * Returns the double value.
     *
     * @param coerce the coerce
     * @return the double value
     * @throws IOException if an I/O error occurs
     */
    double doubleValue(boolean coerce) throws IOException;

    /**
     * Returns the big integer value.
     *
     * @param coerce the coerce
     * @return the big integer value
     * @throws IOException if an I/O error occurs
     */
    BigInteger bigIntegerValue(boolean coerce) throws IOException;

    /**
     * Returns the short value.
     *
     * @return the short value
     * @throws IOException if an I/O error occurs
     */
    short shortValue() throws IOException;

    /**
     * Returns the int value.
     *
     * @return the int value
     * @throws IOException if an I/O error occurs
     */
    int intValue() throws IOException;

    /**
     * Returns the long value.
     *
     * @return the long value
     * @throws IOException if an I/O error occurs
     */
    long longValue() throws IOException;

    /**
     * Returns the float value.
     *
     * @return the float value
     * @throws IOException if an I/O error occurs
     */
    float floatValue() throws IOException;

    /**
     * Returns the double value.
     *
     * @return the double value
     * @throws IOException if an I/O error occurs
     */
    double doubleValue() throws IOException;

    /**
     * Returns the big integer value.
     *
     * @return the big integer value
     * @throws IOException if an I/O error occurs
     */
    BigInteger bigIntegerValue() throws IOException;

    /**
     * Returns the boolean value flag.
     *
     * @return true iff the current value is either boolean (<code>true</code> or <code>false</code>) or one of "false", "true".
     * @throws IOException if an I/O error occurs
     */
    boolean isBooleanValue() throws IOException;

    /**
     * Returns the boolean value.
     *
     * @return the boolean value
     * @throws IOException if an I/O error occurs
     */
    boolean booleanValue() throws IOException;

    /**
     * Reads a plain binary value that was written via one of the following methods:
     *
     * <ul>
     *     <li>{@link XContentBuilder#field(String, byte[], int, int)}}</li>
     *     <li>{@link XContentBuilder#field(String, byte[])}}</li>
     * </ul>
     *
     * as well as via their <code>String</code> variants of the separated value methods.
     * Note: Do not use this method to read values written with:
     * <ul>
     *     <li>{@link XContentBuilder#utf8Value(byte[], int, int)}</li>
     * </ul>
     *
     * these methods write UTF-8 encoded strings and must be read through:
     * <ul>
     *     <li>{@link XContentParser#text()} ()}</li>
     *     <li>{@link XContentParser#textOrNull()} ()}</li>
     *     <li>{@link XContentParser#textCharacters()} ()}}</li>
     * </ul>
     *
     * @return the binary value
     *
     * @throws IOException if an I/O error occurs
     */
    byte[] binaryValue() throws IOException;

    /**
     * Used for error reporting to highlight where syntax errors occur in
     * content being parsed.
     *
     * @return last token's location or null if cannot be determined
     */
    XContentLocation getTokenLocation();

    // TODO remove context entirely when it isn't needed
    /**
     * Parse an object by name.
     *
     * @param <T> the element type
     * @param categoryClass the category class
     * @param name the name
     * @param context the context
     * @return the named object
     * @throws IOException if an I/O error occurs
     */
    <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException;

    /**
     * The registry used to resolve {@link #namedObject(Class, String, Object)}. Use this when building a sub-parser from this parser.
     *
     * @return the XContent registry
     */
    NamedXContentRegistry getXContentRegistry();

    /**
     * Returns the closed flag.
     *
     * @return the closed flag
     */
    boolean isClosed();

    /**
     * The callback to notify when parsing encounters a deprecated field.
     *
     * @return the deprecation handler
     */
    DeprecationHandler getDeprecationHandler();
}
