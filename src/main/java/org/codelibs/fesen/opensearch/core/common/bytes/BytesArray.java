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

package org.codelibs.fesen.opensearch.core.common.bytes;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A bytes array.
 *
 * @opensearch.internal
 */
public final class BytesArray extends AbstractBytesReference {

    /**
     * The EMPTY constant.
     */
    public static final BytesArray EMPTY = new BytesArray(BytesRef.EMPTY_BYTES, 0, 0);
    private final byte[] bytes;
    private final int offset;
    private final int length;

    /**
     * Creates a new BytesArray.
     *
     * @param bytes the bytes
     */
    public BytesArray(String bytes) {
        this(new BytesRef(bytes));
    }

    /**
     * Creates a new BytesArray.
     *
     * @param bytesRef the bytes ref
     */
    public BytesArray(BytesRef bytesRef) {
        this(bytesRef, false);
    }

    /**
     * Creates a new BytesArray.
     *
     * @param bytesRef the bytes ref
     * @param deepCopy the deep copy
     */
    public BytesArray(BytesRef bytesRef, boolean deepCopy) {
        if (deepCopy) {
            bytesRef = BytesRef.deepCopyOf(bytesRef);
        }
        bytes = bytesRef.bytes;
        offset = bytesRef.offset;
        length = bytesRef.length;
    }

    /**
     * Creates a new BytesArray.
     *
     * @param bytes the bytes
     */
    public BytesArray(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    /**
     * Creates a new BytesArray.
     *
     * @param bytes the bytes
     * @param offset the offset
     * @param length the length
     */
    public BytesArray(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return bytes[offset + index];
    }

    @Override
    public int getInt(int index) {
        return (int) BitUtil.VH_BE_INT.get(bytes, offset + index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int hashCode() {
        // NOOP override to satisfy Checkstyle's EqualsHashCode
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof BytesArray that) {
            return Arrays.equals(bytes, offset, offset + length, that.bytes, that.offset, that.offset + that.length);
        }
        return super.equals(other);
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from < 0 || (from + length) > this.length) {
            throw new IllegalArgumentException(
                "can't slice a buffer with length [" + this.length + "], with slice parameters from [" + from + "], length [" + length + "]"
            );
        }
        return new BytesArray(bytes, offset + from, length);
    }

    /**
     * Returns the array.
     *
     * @return the array
     */
    public byte[] array() {
        return bytes;
    }

    /**
     * Returns the offset.
     *
     * @return the offset
     */
    public int offset() {
        return offset;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public long ramBytesUsed() {
        return bytes.length;
    }

    @Override
    public StreamInput streamInput() {
        return StreamInput.wrap(bytes, offset, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes, offset, length);
    }
}
