/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.compress;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;

import org.codelibs.fesen.opensearch.common.compress.DeflateCompressor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A registry that wraps a static Map singleton which holds a mapping of unique String names (typically the
 * compressor header as a string) to registered {@link Compressor} implementations.
 * <p>
 * This enables plugins, modules, extensions to register their own compression implementations through SPI
 *
 * @opensearch.experimental
 * @opensearch.internal
 */
@InternalApi
public final class CompressorRegistry {

    // The backing registry map. This used to be assembled with ServiceLoader so that
    // plugins could contribute compressors. Nothing does: this library is a client that
    // serialises requests and parses responses, and the two providers it shipped were its
    // own. Registering statically keeps the same contents while removing a failure mode
    // that has bitten this fork twice -- a provider pruned as unreachable, the build still
    // green, and the registry silently empty at runtime.
    private static final Map<String, Compressor> registeredCompressors =
        Map.of(NoneCompressor.NAME, new NoneCompressor(), DeflateCompressor.NAME, new DeflateCompressor());

    // no instance:
    private CompressorRegistry() {}

    /**
     * Returns the default compressor
     *
     * @return the default compressor
     */
    public static Compressor defaultCompressor() {
        return registeredCompressors.get("DEFLATE");
    }

    /**
     * Returns the none.
     *
     * @return the none
     */
    public static Compressor none() {
        return registeredCompressors.get(NoneCompressor.NAME);
    }

    /**
     * Returns the compressed flag.
     *
     * @param bytes the bytes
     * @return the compressed flag
     */
    public static boolean isCompressed(BytesReference bytes) {
        return compressor(bytes) != null;
    }

    /**
     * Returns the compressor.
     *
     * @param bytes the bytes
     * @return the compressor
     */
    @Nullable
    public static Compressor compressor(final BytesReference bytes) {
        for (Compressor compressor : registeredCompressors.values()) {
            if (compressor.isCompressed(bytes) == true) {
                // bytes should be either detected as compressed or as xcontent,
                // if we have bytes that can be either detected as compressed or
                // as a xcontent, we have a problem
                assert MediaTypeRegistry.xContentType(bytes) == null;
                return compressor;
            }
        }

        if (MediaTypeRegistry.xContentType(bytes) == null) {
            throw new NotXContentException("Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes");
        }

        return null;
    }

    /**
     * Decompress the provided {@link BytesReference}.
     *
     * @param bytes the bytes
     * @return the uncompress
     * @throws IOException if an I/O error occurs
     */
    public static BytesReference uncompress(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor == null) {
            throw new NotCompressedException();
        }
        return compressor.uncompress(bytes);
    }

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(BytesReference)}.
     *
     * @param bytes the bytes
     * @return the uncompress if needed
     * @throws IOException if an I/O error occurs
     */
    public static BytesReference uncompressIfNeeded(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(Objects.requireNonNull(bytes, "the BytesReference must not be null"));
        return compressor == null ? bytes : compressor.uncompress(bytes);
    }

    /**
     * Returns the registered compressors as an Immutable collection
     * <p>
     * note: used for testing
     *
     * @return the registered compressors
     */
    public static Map<String, Compressor> registeredCompressors() {
        // no destructive danger as backing map is immutable
        return registeredCompressors;
    }
}
