/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.common.io.stream;

import java.io.IOException;

/**
 * Provides a method for serialization which will give ordered stream, creating same byte array on every invocation.
 * This should be invoked with a stream that provides ordered serialization.
 */
public interface VerifiableWriteable extends Writeable {

    /**
     * Writes the verifiable to.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException;
}
