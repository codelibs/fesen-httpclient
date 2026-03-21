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
package org.codelibs.fesen.client.io.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.opensearch.core.common.io.stream.StreamInput;

class ByteArrayStreamOutputTest {

    @Test
    void test_reset() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.writeByte((byte) 1);
        output.writeByte((byte) 2);
        assertEquals(2, output.toByteArray().length);

        output.reset();
        assertEquals(0, output.toByteArray().length);

        // Write after reset
        output.writeByte((byte) 3);
        assertEquals(1, output.toByteArray().length);
        assertEquals(3, output.toByteArray()[0]);
    }

    @Test
    void test_writeByte() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.writeByte((byte) 42);
        final byte[] result = output.toByteArray();
        assertEquals(1, result.length);
        assertEquals(42, result[0]);
    }

    @Test
    void test_writeBytes() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        final byte[] data = { 10, 20, 30, 40, 50 };
        output.writeBytes(data, 1, 3);
        final byte[] result = output.toByteArray();
        assertEquals(3, result.length);
        assertArrayEquals(new byte[] { 20, 30, 40 }, result);
    }

    @Test
    void test_flushAndClose() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.writeByte((byte) 1);
        output.flush();
        assertEquals(1, output.toByteArray().length);
        output.close();
    }

    @Test
    void test_toByteArray() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.writeByte((byte) 0xA);
        output.writeByte((byte) 0xB);
        output.writeByte((byte) 0xC);
        assertArrayEquals(new byte[] { 0xA, 0xB, 0xC }, output.toByteArray());
    }

    @Test
    void test_toStreamInput() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.writeByte((byte) 100);
        output.writeByte((byte) 101);
        final StreamInput input = output.toStreamInput();
        assertNotNull(input);
        assertEquals(100, input.readByte());
        assertEquals(101, input.readByte());
    }

    @Test
    void test_multipleResetCycles() throws IOException {
        final ByteArrayStreamOutput output = new ByteArrayStreamOutput();

        // First cycle
        output.writeByte((byte) 1);
        output.writeByte((byte) 2);
        assertEquals(2, output.toByteArray().length);

        // Reset and second cycle
        output.reset();
        assertEquals(0, output.toByteArray().length);
        output.writeByte((byte) 10);
        assertEquals(1, output.toByteArray().length);
        assertEquals(10, output.toByteArray()[0]);

        // Reset and third cycle
        output.reset();
        assertEquals(0, output.toByteArray().length);
        output.writeBytes(new byte[] { 20, 30, 40 }, 0, 3);
        assertEquals(3, output.toByteArray().length);
        assertArrayEquals(new byte[] { 20, 30, 40 }, output.toByteArray());
    }
}
