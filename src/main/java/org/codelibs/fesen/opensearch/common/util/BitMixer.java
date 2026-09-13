/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * HPPC
 *
 * Copyright (C) 2010-2022 Carrot Search s.c.
 * All rights reserved.
 *
 * Refer to the full license file "LICENSE.txt":
 * https://github.com/carrotsearch/hppc/blob/master/LICENSE.txt
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.common.util;

/**
 * Bit mixing utilities from carrotsearch.hppc.
 * <p>
 * Licensed under ALv2. This is pulled in directly to avoid a full hppc dependency.
 * <p>
 * The purpose of these methods is to evenly distribute key space over int32
 * range.
 */
public final class BitMixer {

    /**
     * Computes David Stafford variant 9 of 64bit mix function (MH3 finalization step,
     * with different shifts and constants).
     * <p>
     * Variant 9 is picked because it contains two 32-bit shifts which could be possibly
     * optimized into better machine code.
     *
     * @see "http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html"
     */
    public static long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >>> 32);
    }

    /*
     * Golden ratio bit mixers.
     */
}
