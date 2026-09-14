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

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.IdentityHashMap;
import org.codelibs.fesen.opensearch.common.xcontent.XContentOpenSearchExtension;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A utility to build XContent (ie json).
 */
@PublicApi(since = "1.0.0")
public final class XContentBuilder implements Closeable, Flushable {

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content.
     * <p>
     * The builder uses an internal {@link ByteArrayOutputStream} output stream to build the content.
     * </p>
     *
     * @param xContent the {@link XContent}
     * @return a new {@link XContentBuilder}
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new ByteArrayOutputStream());
    }

    private static final Map<Class<?>, Writer> WRITERS;
    private static final Map<Class<?>, HumanReadableTransformer> HUMAN_READABLE_TRANSFORMERS;
    private static final Map<Class<?>, Function<Object, Object>> DATE_TRANSFORMERS;
    static {
        Map<Class<?>, Writer> writers = new HashMap<>();
        writers.put(Boolean.class, (b, v) -> b.value((Boolean) v));
        writers.put(Byte.class, (b, v) -> b.value((Byte) v));
        writers.put(byte[].class, (b, v) -> b.value((byte[]) v));
        writers.put(Date.class, XContentBuilder::timeValue);
        writers.put(Double.class, (b, v) -> b.value((Double) v));
        writers.put(double[].class, (b, v) -> b.values((double[]) v));
        writers.put(Float.class, (b, v) -> b.value((Float) v));
        writers.put(float[].class, (b, v) -> b.values((float[]) v));
        writers.put(Integer.class, (b, v) -> b.value((Integer) v));
        writers.put(int[].class, (b, v) -> b.values((int[]) v));
        writers.put(Long.class, (b, v) -> b.value((Long) v));
        writers.put(long[].class, (b, v) -> b.values((long[]) v));
        writers.put(Short.class, (b, v) -> b.value((Short) v));
        writers.put(short[].class, (b, v) -> b.values((short[]) v));
        writers.put(String.class, (b, v) -> b.value((String) v));
        writers.put(String[].class, (b, v) -> b.values((String[]) v));
        writers.put(Locale.class, (b, v) -> b.value(v.toString()));
        writers.put(Class.class, (b, v) -> b.value(v.toString()));
        writers.put(ZonedDateTime.class, (b, v) -> b.value(v.toString()));
        writers.put(Calendar.class, XContentBuilder::timeValue);
        writers.put(GregorianCalendar.class, XContentBuilder::timeValue);
        writers.put(BigInteger.class, (b, v) -> b.value((BigInteger) v));
        writers.put(BigDecimal.class, (b, v) -> b.value((BigDecimal) v));

        Map<Class<?>, HumanReadableTransformer> humanReadableTransformer = new HashMap<>();
        Map<Class<?>, Function<Object, Object>> dateTransformers = new HashMap<>();

        // treat strings as already converted
        dateTransformers.put(String.class, Function.identity());

        // The single extension, registered directly. This was a ServiceLoader lookup so that
        // core did not have to name the xcontent library; there was only ever one
        // implementation and it ships in this same artifact, so the indirection only created
        // a way for the date/byte-size writers to disappear silently if it were pruned.
        for (XContentBuilderExtension service : List.of(new XContentOpenSearchExtension())) {
            Map<Class<?>, Writer> addlWriters = service.getXContentWriters();
            Map<Class<?>, HumanReadableTransformer> addlTransformers = service.getXContentHumanReadableTransformers();
            Map<Class<?>, Function<Object, Object>> addlDateTransformers = service.getDateTransformers();

            addlWriters.forEach((key, value) -> Objects.requireNonNull(value, "invalid null xcontent writer for class " + key));
            addlTransformers.forEach(
                (key, value) -> Objects.requireNonNull(value, "invalid null xcontent transformer for human readable class " + key)
            );
            dateTransformers.forEach(
                (key, value) -> Objects.requireNonNull(value, "invalid null xcontent date transformer for class " + key)
            );

            writers.putAll(addlWriters);
            humanReadableTransformer.putAll(addlTransformers);
            dateTransformers.putAll(addlDateTransformers);
        }

        WRITERS = Collections.unmodifiableMap(writers);
        HUMAN_READABLE_TRANSFORMERS = Collections.unmodifiableMap(humanReadableTransformer);
        DATE_TRANSFORMERS = Collections.unmodifiableMap(dateTransformers);
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     * Note: explicitly or implicitly (from debugger) calling toString() could cause XContentBuilder
     * to close which is a side effect done by @see BytesReference#bytes().
     * Trying to write more contents after toString() will cause NPE. Use it with caution.
     */
    @Override
    public String toString() {
        return BytesReference.bytes(this).utf8ToString();
    }

    /**
     * The writer interface for the serializable content builder
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface Writer {
        /**
         * Writes this instance.
         *
         * @param builder the content builder
         * @param value the value
         * @throws IOException if an I/O error occurs
         */
        void write(XContentBuilder builder, Object value) throws IOException;
    }

    /**
     * Interface for transforming complex objects into their "raw" equivalents for human-readable fields
     */
    @FunctionalInterface
    public interface HumanReadableTransformer {
        /**
         * Returns the raw value.
         *
         * @param value the value
         * @return the raw value
         * @throws IOException if an I/O error occurs
         */
        Object rawValue(Object value) throws IOException;
    }

    /**
     * XContentGenerator used to build the XContent object
     */
    private XContentGenerator generator;

    /**
     * Use pretty print ("false" by default)
     */
    private boolean prettyPrint;

    /**
     * Output stream to which the built object is written
     */
    private final OutputStream bos;

    /**
     * The inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     */
    private final Set<String> includes;

    /**
     * The exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     */
    private final Set<String> excludes;

    /**
     * XContent instance
     */
    private final XContent xContent;

    /**
     * When this flag is set to true, some types of values are written in a format easier to read for a human.
     */
    private boolean humanReadable = false;

    /**
     * Constructs a new builder using the provided XContent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     *
     * @param xContent the XContent
     * @param bos the bos
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Creates a new builder using the provided XContent, output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     * Make sure to call {@link #close()} when the builder is done with.
     *
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @param xContent the XContent
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder(XContent xContent, OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        this(xContent, os, includes, excludes, null, false);
    }

    /**
     * Creates a new builder using the provided XContent, output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     * Make sure to call {@link #close()} when the builder is done with.
     *
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @param parent references the parent this instance was copied from
     * @param prettyPrint use pretty printer
     */
    private XContentBuilder(
        XContent xContent,
        OutputStream os,
        Set<String> includes,
        Set<String> excludes,
        XContentBuilder parent,
        boolean prettyPrint
    ) throws IOException {
        this.xContent = xContent;
        this.bos = os;
        this.includes = includes;
        this.excludes = excludes;
        this.prettyPrint = prettyPrint;
    }

    /**
     * Since 3.x release line, Jackson does not allow on the fly changes to the generator
     * (like changing pretty printer, etc). To workaround and preserve the APIs, we do defer
     * the generator instantiation till the first usage.
     */
    private XContentGenerator generatorInstance() throws IOException {
        if (generator == null) {
            generator = xContent.createGenerator(bos, includes, excludes, prettyPrint);
        }
        return generator;
    }

    /**
     * Returns the content type.
     *
     * @return the content type
     */
    public MediaType contentType() {
        try {
            return generatorInstance().contentType();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Returns the output stream.
     *
     * @return the output stream to which the built object is being written. Note that is dangerous to modify the stream.
     */
    public OutputStream getOutputStream() {
        return bos;
    }

    /**
     * Returns the pretty print.
     *
     * @return the pretty print
     */
    public XContentBuilder prettyPrint() {
        if (this.prettyPrint == false && generator != null) {
            throw new IllegalStateException("Cannot change the prettyPrint status, the generator has been initialized already");
        }

        this.prettyPrint = true;
        return this;
    }

    /**
     * Set the "human readable" flag. Once set, some types of values are written in a
     * format easier to read for a human.
     *
     * @param humanReadable the human readable
     * @return the human readable
     */
    public XContentBuilder humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    /**
     * Returns the human readable.
     *
     * @return the value of the "human readable" flag. When the value is equal to true,
     * some types of values are written in a format easier to read for a human.
     */
    public boolean humanReadable() {
        return this.humanReadable;
    }

    // ------------------------------------------------------------------------
    // Structure (object, array, field, null values...)
    // ------------------------------

    /**
     * Starts the object.
     *
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder startObject() throws IOException {
        generatorInstance().writeStartObject();
        return this;
    }

    /**
     * Starts the object.
     *
     * @param name the name
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder startObject(String name) throws IOException {
        return field(name).startObject();
    }

    /**
     * Returns the end object.
     *
     * @return the end object
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder endObject() throws IOException {
        generatorInstance().writeEndObject();
        return this;
    }

    /**
     * Starts the array.
     *
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder startArray() throws IOException {
        generatorInstance().writeStartArray();
        return this;
    }

    /**
     * Starts the array.
     *
     * @param name the name
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder startArray(String name) throws IOException {
        return field(name).startArray();
    }

    /**
     * Returns the end array.
     *
     * @return the end array
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder endArray() throws IOException {
        generatorInstance().writeEndArray();
        return this;
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeFieldName(name);
        return this;
    }

    /**
     * Returns the null field.
     *
     * @param name the name
     * @return the null field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder nullField(String name) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeNullField(name);
        return this;
    }

    /**
     * Returns the null value.
     *
     * @return the null value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder nullValue() throws IOException {
        generatorInstance().writeNull();
        return this;
    }

    // ------------------------------------------------------------------------
    // Boolean
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Boolean value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.booleanValue());
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, boolean value) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeBooleanField(name, value);
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Boolean value) throws IOException {
        return (value == null) ? nullValue() : value(value.booleanValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(boolean value) throws IOException {
        generatorInstance().writeBoolean(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Byte
    // ------------------------------

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Byte value) throws IOException {
        return (value == null) ? nullValue() : value(value.byteValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(byte value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Double
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Double value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.doubleValue());
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, double value) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeNumberField(name, value);
        return this;
    }

    /**
     * Returns the array.
     *
     * @param name the name
     * @param values the values
     * @return the array
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder array(String name, double[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(double[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (double b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Double value) throws IOException {
        return (value == null) ? nullValue() : value(value.doubleValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(double value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Float
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Float value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.floatValue());
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, float value) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(float[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (float f : values) {
            value(f);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Float value) throws IOException {
        return (value == null) ? nullValue() : value(value.floatValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(float value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Integer
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Integer value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.intValue());
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, int value) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(int[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (int i : values) {
            value(i);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Integer value) throws IOException {
        return (value == null) ? nullValue() : value(value.intValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(int value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Long
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Long value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.longValue());
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, long value) throws IOException {
        ensureNameNotNull(name);
        generatorInstance().writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(long[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (long l : values) {
            value(l);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Long value) throws IOException {
        return (value == null) ? nullValue() : value(value.longValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(long value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Short
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, short value) throws IOException {
        return field(name).value(value);
    }

    private XContentBuilder values(short[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (short s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Short value) throws IOException {
        return (value == null) ? nullValue() : value(value.shortValue());
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(short value) throws IOException {
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // BigInteger
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, BigInteger value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generatorInstance().writeNumberField(name, value);
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(BigInteger value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // BigDecimal
    // ------------------------------

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(BigDecimal value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generatorInstance().writeNumber(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // String
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, String value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generatorInstance().writeStringField(name, value);
        return this;
    }

    /**
     * Returns the array.
     *
     * @param name the name
     * @param values the values
     * @return the array
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder array(String name, String... values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(String[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (String s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generatorInstance().writeString(value);
        return this;
    }

    // ------------------------------------------------------------------------
    // Binary
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, byte[] value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generatorInstance().writeBinaryField(name, value);
        return this;
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(byte[] value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generatorInstance().writeBinary(value);
        return this;
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @param offset the offset
     * @param length the length
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, byte[] value, int offset, int length) throws IOException {
        return field(name).value(value, offset, length);
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @param offset the offset
     * @param length the length
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(byte[] value, int offset, int length) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generatorInstance().writeBinary(value, offset, length);
        return this;
    }

    /**
     * Writes the binary content of the given byte array as UTF-8 bytes.
     * <p>
     * Use {@link XContentParser#charBuffer()} to read the value back
     *
     * @param bytes the bytes
     * @param offset the offset
     * @param length the length
     * @return the utf8 value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder utf8Value(byte[] bytes, int offset, int length) throws IOException {
        generatorInstance().writeUTF8String(bytes, offset, length);
        return this;
    }

    // ------------------------------------------------------------------------
    // Date
    // ------------------------------

    /**
     * If the {@code humanReadable} flag is set, writes both a formatted and
     * unformatted version of the time value using the date transformer for the
     * {@link Long} class.
     *
     * @param name the name
     * @param readableName the readable name
     * @param value the value
     * @return the time field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder timeField(String name, String readableName, long value) throws IOException {
        assert name.equals(readableName) == false : "expected raw and readable field names to differ, but they were both: " + name;
        if (humanReadable) {
            Function<Object, Object> longTransformer = DATE_TRANSFORMERS.get(Long.class);
            if (longTransformer == null) {
                throw new IllegalArgumentException("cannot write time value xcontent for unknown value of type Long");
            }
            field(readableName).value(longTransformer.apply(value));
        }
        field(name, value);
        return this;
    }

    /**
     * Write a time-based value, if the value is null a null value is written,
     * otherwise a date transformers lookup is performed.

     * @param timeValue the time value
     * @return the time value
     * @throws IllegalArgumentException if there is no transformers for the type of object
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder timeValue(Object timeValue) throws IOException {
        if (timeValue == null) {
            return nullValue();
        } else {
            Function<Object, Object> transformer = DATE_TRANSFORMERS.get(timeValue.getClass());
            if (transformer == null) {
                throw new IllegalArgumentException("cannot write time value xcontent for unknown value of type " + timeValue.getClass());
            }
            return value(transformer.apply(timeValue));
        }
    }

    // ------------------------------------------------------------------------
    // LatLon
    // ------------------------------

    /**
     * Returns the latlon.
     *
     * @param lat the lat
     * @param lon the lon
     * @return the latlon
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder latlon(double lat, double lon) throws IOException {
        return startObject().field("lat", lat).field("lon", lon).endObject();
    }

    // ------------------------------------------------------------------------
    // Path
    // ------------------------------

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Path value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.toString());
    }

    // ------------------------------------------------------------------------
    // Objects
    //
    // These methods are used when the type of value is unknown. It tries to fallback
    // on typed methods and use Object.toString() as a last resort. Always prefer using
    // typed methods over this.
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Object value) throws IOException {
        return field(name).value(value);
    }

    /**
     * Returns the array.
     *
     * @param name the name
     * @param values the values
     * @return the array
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder array(String name, Object... values) throws IOException {
        return field(name).values(values, true);
    }

    private XContentBuilder values(Object[] values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            return nullValue();
        }
        return value(Arrays.asList(values), ensureNoSelfReferences);
    }

    /**
     * Returns the value.
     *
     * @param value the value
     * @return the value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder value(Object value) throws IOException {
        unknownValue(value, true);
        return this;
    }

    private void unknownValue(Object value, boolean ensureNoSelfReferences) throws IOException {
        if (value == null) {
            nullValue();
            return;
        }
        Writer writer = WRITERS.get(value.getClass());
        if (writer != null) {
            writer.write(this, value);
        } else if (value instanceof Path path) {
            // Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            value(path);
        } else if (value instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> valueMap = (Map<String, ?>) value;
            map(valueMap, ensureNoSelfReferences, true);
        } else if (value instanceof Iterable<?> iterable) {
            value(iterable, ensureNoSelfReferences);
        } else if (value instanceof Object[] objectArray) {
            values(objectArray, ensureNoSelfReferences);
        } else if (value instanceof ToXContent toXContent) {
            value(toXContent);
        } else if (value instanceof Enum<?>) {
            // Write out the Enum toString
            value(Objects.toString(value));
        } else {
            throw new IllegalArgumentException("cannot write xcontent for unknown value of type " + value.getClass());
        }
    }

    // ------------------------------------------------------------------------
    // ToXContent
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, ToXContent value) throws IOException {
        return field(name).value(value);
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param value the value
     * @param params the serialization parameters
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, ToXContent value, ToXContent.Params params) throws IOException {
        return field(name).value(value, params);
    }

    private XContentBuilder value(ToXContent value) throws IOException {
        return value(value, ToXContent.EMPTY_PARAMS);
    }

    private XContentBuilder value(ToXContent value, ToXContent.Params params) throws IOException {
        if (value == null) {
            return nullValue();
        }
        value.toXContent(this, params);
        return this;
    }

    // ------------------------------------------------------------------------
    // Maps & Iterable
    // ------------------------------

    /**
     * Returns the field.
     *
     * @param name the name
     * @param values the values
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Map<String, Object> values) throws IOException {
        return field(name).map(values);
    }

    /**
     * Returns the map.
     *
     * @param values the values
     * @return the map
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder map(Map<String, ?> values) throws IOException {
        return map(values, true, true);
    }

    /**
     * writes a map without the start object and end object headers
     *
     * @param values the values
     * @return the map contents
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder mapContents(Map<String, ?> values) throws IOException {
        return map(values, true, false);
    }

    private XContentBuilder map(Map<String, ?> values, boolean ensureNoSelfReferences, boolean writeStartAndEndHeaders) throws IOException {
        if (values == null) {
            return nullValue();
        }

        // checks that the map does not contain references to itself because
        // iterating over map entries will cause a stackoverflow error
        if (ensureNoSelfReferences) {
            ensureNoSelfReferences(values);
        }

        if (writeStartAndEndHeaders) {
            startObject();
        }
        for (Map.Entry<String, ?> value : values.entrySet()) {
            field(value.getKey());
            // pass ensureNoSelfReferences=false as we already performed the check at a higher level
            unknownValue(value.getValue(), false);
        }
        if (writeStartAndEndHeaders) {
            endObject();
        }
        return this;
    }

    /**
     * Returns the field.
     *
     * @param name the name
     * @param values the values
     * @return the field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder field(String name, Iterable<?> values) throws IOException {
        return field(name).value(values);
    }

    private XContentBuilder value(Iterable<?> values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            return nullValue();
        }

        if (values instanceof Path path) {
            // treat as single value
            value(path);
        } else {
            // checks that the iterable does not contain references to itself because
            // iterating over entries will cause a stackoverflow error
            if (ensureNoSelfReferences) {
                ensureNoSelfReferences(values);
            }
            startArray();
            for (Object value : values) {
                // pass ensureNoSelfReferences=false as we already performed the check at a higher level
                unknownValue(value, false);
            }
            endArray();
        }
        return this;
    }

    // ------------------------------------------------------------------------
    // Human readable fields
    //
    // These are fields that have a "raw" value and a "human readable" value,
    // such as time values or byte sizes. The human readable variant is only
    // used if the humanReadable flag has been set
    // ------------------------------

    /**
     * Returns the human readable field.
     *
     * @param rawFieldName the raw field name
     * @param readableFieldName the readable field name
     * @param value the value
     * @return the human readable field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder humanReadableField(String rawFieldName, String readableFieldName, Object value) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false : "expected raw and readable field names to differ, but they were both: "
            + rawFieldName;
        if (humanReadable) {
            field(readableFieldName, Objects.toString(value));
        }
        HumanReadableTransformer transformer = HUMAN_READABLE_TRANSFORMERS.get(value.getClass());
        if (transformer != null) {
            Object rawValue = transformer.rawValue(value);
            field(rawFieldName, rawValue);
        } else {
            throw new IllegalArgumentException("no raw transformer found for class " + value.getClass());
        }
        return this;
    }

    // ------------------------------------------------------------------------
    // Misc.
    // ------------------------------

    /**
     * Returns the percentage field.
     *
     * @param rawFieldName the raw field name
     * @param readableFieldName the readable field name
     * @param percentage the percentage
     * @return the percentage field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder percentageField(String rawFieldName, String readableFieldName, double percentage) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false : "expected raw and readable field names to differ, but they were both: "
            + rawFieldName;
        if (humanReadable) {
            field(readableFieldName, String.format(Locale.ROOT, "%1.1f%%", percentage));
        }
        field(rawFieldName, percentage);
        return this;
    }

    // ------------------------------------------------------------------------
    // Raw fields
    // ------------------------------

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @param name the name
     * @param value the value
     * @return the raw field
     * @throws IOException if an I/O error occurs
     * @deprecated use {@link #rawField(String, InputStream, MediaType)} to avoid content type auto-detection
     */
    @Deprecated
    public XContentBuilder rawField(String name, InputStream value) throws IOException {
        generatorInstance().writeRawField(name, value);
        return this;
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     *
     * @param name the name
     * @param value the value
     * @param mediaType the media type
     * @return the raw field
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder rawField(String name, InputStream value, MediaType mediaType) throws IOException {
        generatorInstance().writeRawField(name, value, mediaType);
        return this;
    }

    /**
     * Writes a value with the source coming directly from the bytes in the stream
     *
     * @param stream the stream
     * @param contentType the content type
     * @return the raw value
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder rawValue(InputStream stream, MediaType contentType) throws IOException {
        generatorInstance().writeRawValue(stream, contentType);
        return this;
    }

    /**
     * Copies the current structure.
     *
     * @param parser the parser
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder copyCurrentStructure(XContentParser parser) throws IOException {
        generatorInstance().copyCurrentStructure(parser);
        return this;
    }

    @Override
    public void flush() throws IOException {
        generatorInstance().flush();
    }

    @Override
    public void close() {
        try {
            generatorInstance().close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close the XContentBuilder", e);
        }
    }

    /**
     * Returns the generator.
     *
     * @return the generator
     */
    public XContentGenerator generator() {
        try {
            return generatorInstance();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

    }

    /**
     * Ensures the name not null.
     *
     * @param name the name
     */
    public static void ensureNameNotNull(String name) {
        ensureNotNull(name, "Field name cannot be null");
    }

    /**
     * Ensures the not null.
     *
     * @param value the value
     * @param message the message
     */
    public static void ensureNotNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void ensureNoSelfReferences(Object value) {
        Iterable<?> it = convert(value);
        if (it != null) {
            ensureNoSelfReferences(it, value, Collections.newSetFromMap(new IdentityHashMap<>()));
        }
    }

    private static Iterable<?> convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return map.values();
        } else if (value instanceof Iterable<?> iterable && value instanceof Path == false) {
            return iterable;
        } else if (value instanceof Object[] objectArray) {
            return Arrays.asList(objectArray);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(final Iterable<?> value, Object originalReference, final Set<Object> ancestors) {
        if (value != null) {
            if (ancestors.add(originalReference) == false) {
                throw new IllegalArgumentException("Iterable object is self-referencing itself");
            }
            for (Object o : value) {
                ensureNoSelfReferences(convert(o), o, ancestors);
            }
            ancestors.remove(originalReference);
        }
    }

}
