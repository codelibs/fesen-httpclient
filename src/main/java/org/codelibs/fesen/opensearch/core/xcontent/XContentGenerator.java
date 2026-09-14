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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Base class to generate serializable content
 *
 * @opensearch.internal
 */
public interface XContentGenerator extends Closeable, Flushable {

    /**
     * Returns the content type.
     *
     * @return the content type
     */
    MediaType contentType();

    /**
     * Returns the pretty print flag.
     *
     * @return the pretty print flag
     */
    boolean isPrettyPrint();

    /**
     * Performs the use print line feed at end step.
     */
    void usePrintLineFeedAtEnd();

    /**
     * Writes the start object.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeStartObject() throws IOException;

    /**
     * Writes the end object.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeEndObject() throws IOException;

    /**
     * Writes the start array.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeStartArray() throws IOException;

    /**
     * Writes the end array.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeEndArray() throws IOException;

    /**
     * Writes the field name.
     *
     * @param name the name
     * @throws IOException if an I/O error occurs
     */
    void writeFieldName(String name) throws IOException;

    /**
     * Writes the null.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeNull() throws IOException;

    /**
     * Writes the null field.
     *
     * @param name the name
     * @throws IOException if an I/O error occurs
     */
    void writeNullField(String name) throws IOException;

    /**
     * Writes the boolean field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeBooleanField(String name, boolean value) throws IOException;

    /**
     * Writes the boolean.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeBoolean(boolean value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, double value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(double value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, float value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(float value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, int value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(int value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, long value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(long value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(short value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(BigInteger value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, BigInteger value) throws IOException;

    /**
     * Writes the number.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumber(BigDecimal value) throws IOException;

    /**
     * Writes the number field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeNumberField(String name, BigDecimal value) throws IOException;

    /**
     * Writes the string field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeStringField(String name, String value) throws IOException;

    /**
     * Writes the string.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeString(String value) throws IOException;

    /**
     * Writes the string.
     *
     * @param text the text
     * @param offset the offset
     * @param len the len
     * @throws IOException if an I/O error occurs
     */
    void writeString(char[] text, int offset, int len) throws IOException;

    /**
     * Writes the UTF 8 string.
     *
     * @param value the value
     * @param offset the offset
     * @param length the length
     * @throws IOException if an I/O error occurs
     */
    void writeUTF8String(byte[] value, int offset, int length) throws IOException;

    /**
     * Writes the binary field.
     *
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeBinaryField(String name, byte[] value) throws IOException;

    /**
     * Writes the binary.
     *
     * @param value the value
     * @throws IOException if an I/O error occurs
     */
    void writeBinary(byte[] value) throws IOException;

    /**
     * Writes the binary.
     *
     * @param value the value
     * @param offset the offset
     * @param length the length
     * @throws IOException if an I/O error occurs
     */
    void writeBinary(byte[] value, int offset, int length) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @param name the name
     * @param value the value
     * @throws IOException if an I/O error occurs
     * @deprecated use {@link #writeRawField(String, InputStream, MediaType)} to avoid content type auto-detection
     */
    @Deprecated
    void writeRawField(String name, InputStream value) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     *
     * @param name the name
     * @param value the value
     * @param mediaType the media type
     * @throws IOException if an I/O error occurs
     */
    void writeRawField(String name, InputStream value, MediaType mediaType) throws IOException;

    /**
     * Writes a raw value taken from the bytes in the stream
     *
     * @param value the value
     * @param mediaType the media type
     * @throws IOException if an I/O error occurs
     */
    void writeRawValue(InputStream value, MediaType mediaType) throws IOException;

    /**
     * Copies the current structure.
     *
     * @param parser the parser
     * @throws IOException if an I/O error occurs
     */
    void copyCurrentStructure(XContentParser parser) throws IOException;

    /**
     * Copies the current event.
     *
     * @param parser the parser
     * @throws IOException if an I/O error occurs
     */
    default void copyCurrentEvent(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                writeStartObject();
                break;
            case END_OBJECT:
                writeEndObject();
                break;
            case START_ARRAY:
                writeStartArray();
                break;
            case END_ARRAY:
                writeEndArray();
                break;
            case FIELD_NAME:
                writeFieldName(parser.currentName());
                break;
            case VALUE_STRING:
                if (parser.hasTextCharacters()) {
                    writeString(parser.textCharacters(), parser.textOffset(), parser.textLength());
                } else {
                    writeString(parser.text());
                }
                break;
            case VALUE_NUMBER:
                switch (parser.numberType()) {
                    case INT:
                        writeNumber(parser.intValue());
                        break;
                    case LONG:
                        writeNumber(parser.longValue());
                        break;
                    case FLOAT:
                        writeNumber(parser.floatValue());
                        break;
                    case DOUBLE:
                        writeNumber(parser.doubleValue());
                        break;
                }
                break;
            case VALUE_BOOLEAN:
                writeBoolean(parser.booleanValue());
                break;
            case VALUE_NULL:
                writeNull();
                break;
            case VALUE_EMBEDDED_OBJECT:
                writeBinary(parser.binaryValue());
        }
    }

    /**
     * Returns {@code true} if this XContentGenerator has been closed. A closed generator can not do any more output.
     *
     * @return the closed flag
     */
    boolean isClosed();

}
