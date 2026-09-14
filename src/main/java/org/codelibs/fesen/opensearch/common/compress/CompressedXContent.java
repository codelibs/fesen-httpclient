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

package org.codelibs.fesen.opensearch.common.compress;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.io.Streams;
import org.codelibs.fesen.opensearch.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.compress.Compressor;
import org.codelibs.fesen.opensearch.core.compress.CompressorRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class CompressedXContent {

    private static int crc32(BytesReference data) {
        CRC32 crc32 = new CRC32();
        try {
            data.writeTo(new CheckedOutputStream(Streams.NULL_OUTPUT_STREAM, crc32));
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return (int) crc32.getValue();
    }

    private final byte[] bytes;
    private final int crc32;

    // Used for serialization
    private CompressedXContent(byte[] compressed, int crc32) {
        this.bytes = compressed;
        this.crc32 = crc32;
        assertConsistent();
    }

    /**
     * Create a {@link CompressedXContent} out of a serialized {@link ToXContent}
     * that may already be compressed.
     *
     * @param data the data
     * @throws IOException if an I/O error occurs
     */
    public CompressedXContent(BytesReference data) throws IOException {
        Compressor compressor = CompressorRegistry.compressor(data);
        if (compressor != null) {
            // already compressed...
            this.bytes = BytesReference.toBytes(data);
            this.crc32 = crc32(uncompressed());
        } else {
            this.bytes = BytesReference.toBytes(CompressorRegistry.defaultCompressor().compress(data));
            this.crc32 = crc32(data);
        }
        assertConsistent();
    }

    private void assertConsistent() {
        assert CompressorRegistry.compressor(new BytesArray(bytes)) != null;
        assert this.crc32 == crc32(uncompressed());
    }

    /**
     * Creates a new CompressedXContent.
     *
     * @param data the data
     * @throws IOException if an I/O error occurs
     */
    public CompressedXContent(byte[] data) throws IOException {
        this(new BytesArray(data));
    }

    /**
     * Creates a new CompressedXContent.
     *
     * @param str the str
     * @throws IOException if an I/O error occurs
     */
    public CompressedXContent(String str) throws IOException {
        this(new BytesArray(str.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Return the compressed bytes.
     *
     * @return the compressed
     */
    public byte[] compressed() {
        return this.bytes;
    }

    /**
     * Return the compressed bytes as a {@link BytesReference}.
     *
     * @return the compressed reference
     */
    public BytesReference compressedReference() {
        return new BytesArray(bytes);
    }

    /**
     * Return the uncompressed bytes.
     *
     * @return the uncompressed
     */
    public BytesReference uncompressed() {
        try {
            return CompressorRegistry.uncompress(new BytesArray(bytes));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed string", e);
        }
    }

    /**
     * Returns the string.
     *
     * @return the string
     */
    public String string() {
        return uncompressed().utf8ToString();
    }

    /**
     * Reads the compressed string.
     *
     * @param in the input to read from
     * @return the compressed string
     * @throws IOException if an I/O error occurs
     */
    public static CompressedXContent readCompressedString(StreamInput in) throws IOException {
        int crc32 = in.readInt();
        return new CompressedXContent(in.readByteArray(), crc32);
    }

    /**
     * Writes this instance to the given output.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(crc32);
        out.writeByteArray(bytes);
    }

    /**
     * Writes the verifiable to.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        out.writeInt(crc32);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedXContent that = (CompressedXContent) o;

        if (Arrays.equals(compressed(), that.compressed())) {
            return true;
        }

        if (crc32 != that.crc32) {
            return false;
        }

        return uncompressed().equals(that.uncompressed());
    }

    @Override
    public int hashCode() {
        return crc32;
    }

    @Override
    public String toString() {
        return string();
    }
}
