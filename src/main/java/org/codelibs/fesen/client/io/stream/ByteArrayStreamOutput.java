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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A {@link StreamOutput} implementation backed by a {@link ByteArrayOutputStream}.
 * The written content can be retrieved as a byte array or converted to a {@link StreamInput}.
 */
public class ByteArrayStreamOutput extends StreamOutput {
    private final ByteArrayOutputStream out;

    /**
     * Creates a new stream output backed by an empty byte array output stream.
     */
    public ByteArrayStreamOutput() {
        this.out = new ByteArrayOutputStream();
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        out.write(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        out.write(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        out.reset();
    }

    /**
     * Returns a copy of the bytes written to this stream.
     *
     * @return the written content as a byte array
     */
    public byte[] toByteArray() {
        return out.toByteArray();
    }

    /**
     * Creates a {@link StreamInput} that reads the bytes written to this stream.
     *
     * @return a stream input over the written content
     */
    public StreamInput toStreamInput() {
        return new InputStreamStreamInput(new ByteArrayInputStream(toByteArray()));
    }
}
