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

package org.codelibs.fesen.opensearch.common.xcontent;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.compress.Compressor;
import org.codelibs.fesen.opensearch.core.compress.CompressorRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent.Params;
import org.codelibs.fesen.opensearch.core.xcontent.XContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParseException;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper for xcontent utilities.
 *
 * @opensearch.internal
 */
@SuppressWarnings("unchecked")
public class XContentHelper {

    /**
     * Creates a parser based on the bytes provided
     * @deprecated use {@link #createParser(NamedXContentRegistry, DeprecationHandler, BytesReference, MediaType)}
     * to avoid content type auto-detection
     */
    @Deprecated
    public static XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        BytesReference bytes
    ) throws IOException {
        Compressor compressor = CompressorRegistry.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = null;
            try {
                compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedInput.markSupported() == false) {
                    compressedInput = new BufferedInputStream(compressedInput);
                }
                final MediaType contentType = MediaTypeRegistry.xContentType(compressedInput);
                return contentType.xContent().createParser(xContentRegistry, deprecationHandler, compressedInput);
            } catch (Exception e) {
                if (compressedInput != null) compressedInput.close();
                throw e;
            }
        } else {
            return MediaTypeRegistry.xContentType(bytes).xContent().createParser(xContentRegistry, deprecationHandler, bytes.streamInput());
        }
    }

    /**
     * Creates a parser for the bytes using the supplied content-type
     */
    public static XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        BytesReference bytes,
        MediaType mediaType
    ) throws IOException {
        Objects.requireNonNull(mediaType);
        Compressor compressor = CompressorRegistry.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = null;
            try {
                compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedInput.markSupported() == false) {
                    compressedInput = new BufferedInputStream(compressedInput);
                }
                return mediaType.xContent().createParser(xContentRegistry, deprecationHandler, compressedInput);
            } catch (Exception e) {
                if (compressedInput != null) compressedInput.close();
                throw e;
            }
        } else {
            if (bytes instanceof BytesArray array) {
                return mediaType.xContent()
                    .createParser(xContentRegistry, deprecationHandler, array.array(), array.offset(), array.length());
            }
            return mediaType.xContent().createParser(xContentRegistry, deprecationHandler, bytes.streamInput());
        }
    }

    /**
     * Converts the given bytes into a map that is optionally ordered.
     * @deprecated this method relies on auto-detection of content type. Use {@link #convertToMap(BytesReference, boolean, MediaType)}
     *             instead with the proper {@link XContentType}
     */
    @Deprecated
    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered)
        throws OpenSearchParseException {
        return convertToMap(bytes, ordered, null);
    }

    /**
     * Converts the given bytes into a map that is optionally ordered. The provided {@link XContentType} must be non-null.
     *
     * @deprecated use {@link #convertToMap(BytesReference, boolean, MediaType)} instead
     */
    @Deprecated
    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered, XContentType xContentType) {
        if (Objects.isNull(xContentType) == false && xContentType instanceof XContentType == false) {
            throw new IllegalArgumentException(
                "XContentHelper.convertToMap does not support media type [" + xContentType.getClass().getName() + "]"
            );
        }
        return (Tuple<XContentType, Map<String, Object>>) convertToMap(bytes, ordered, (MediaType) xContentType);
    }

    /**
     * Converts the given bytes into a map that is optionally ordered. The provided {@link XContentType} must be non-null.
     */
    public static Tuple<? extends MediaType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered, MediaType mediaType)
        throws OpenSearchParseException {
        try {
            final MediaType contentType;
            InputStream input;
            Compressor compressor = CompressorRegistry.compressor(bytes);
            if (compressor != null) {
                InputStream compressedStreamInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedStreamInput.markSupported() == false) {
                    compressedStreamInput = new BufferedInputStream(compressedStreamInput);
                }
                input = compressedStreamInput;
            } else if (bytes instanceof BytesArray arr) {
                final byte[] raw = arr.array();
                final int offset = arr.offset();
                final int length = arr.length();
                contentType = mediaType != null ? mediaType : MediaTypeRegistry.mediaTypeFromBytes(raw, offset, length);
                return new Tuple<>(Objects.requireNonNull(contentType), convertToMap(contentType.xContent(), raw, offset, length, ordered));
            } else {
                input = bytes.streamInput();
            }
            try (InputStream stream = input) {
                contentType = mediaType != null ? mediaType : MediaTypeRegistry.xContentType(stream);
                return new Tuple<>(Objects.requireNonNull(contentType), convertToMap(contentType.xContent(), stream, ordered));
            }
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link OpenSearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, InputStream input, boolean ordered) throws OpenSearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                input
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a byte array in some {@link XContent} format to a {@link Map}. Throws an {@link OpenSearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, byte[] bytes, int offset, int length, boolean ordered)
        throws OpenSearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes,
                offset,
                length
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson) throws IOException {
        return convertToJson(bytes, reformatJson, false);
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint) throws IOException {
        return convertToJson(bytes, reformatJson, prettyPrint, MediaTypeRegistry.xContent(bytes.toBytesRef().bytes));
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson, MediaType mediaType) throws IOException {
        return convertToJson(bytes, reformatJson, false, mediaType);
    }

    /**
     * Converts the given {@link MediaType} to a json string
     */
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint, MediaType mediaType)
        throws IOException {
        Objects.requireNonNull(mediaType);
        if (mediaType == MediaTypeRegistry.JSON && !reformatJson) {
            return bytes.utf8ToString();
        }

        // It is safe to use EMPTY here because this never uses namedObject
        if (bytes instanceof BytesArray array) {
            try (
                XContentParser parser = mediaType.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        array.array(),
                        array.offset(),
                        array.length()
                    )
            ) {
                return toJsonString(prettyPrint, parser);
            }
        } else {
            try (
                InputStream stream = bytes.streamInput();
                XContentParser parser = mediaType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)
            ) {
                return toJsonString(prettyPrint, parser);
            }
        }
    }

    private static String toJsonString(boolean prettyPrint, XContentParser parser) throws IOException {
        parser.nextToken();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        if (prettyPrint) {
            builder.prettyPrint();
        }
        builder.copyCurrentStructure(parser);
        return builder.toString();
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, InputStream)}.
     * @deprecated use {@code #writeRawField(String, BytesReference, XContentType, XContentBuilder, Params)} to avoid content type
     * auto-detection
     */
    @Deprecated
    public static void writeRawField(String field, BytesReference source, XContentBuilder builder, Params params) throws IOException {
        Compressor compressor = CompressorRegistry.compressor(source);
        if (compressor != null) {
            try (InputStream compressedStreamInput = compressor.threadLocalInputStream(source.streamInput())) {
                builder.rawField(field, compressedStreamInput);
            }
        } else {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(field, stream);
            }
        }
    }
}
