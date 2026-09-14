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

package org.codelibs.fesen.opensearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.lease.Releasable;
import org.codelibs.fesen.opensearch.common.lease.Releasables;
import org.codelibs.fesen.opensearch.common.recycler.Recycler;
import org.codelibs.fesen.opensearch.core.common.breaker.CircuitBreaker;
import org.codelibs.fesen.opensearch.core.common.breaker.CircuitBreakingException;
import org.codelibs.fesen.opensearch.core.common.util.BigArray;
import org.codelibs.fesen.opensearch.core.common.util.ByteArray;
import org.codelibs.fesen.opensearch.core.indices.breaker.CircuitBreakerService;

import java.util.Arrays;

/**
 * Utility class to work with arrays.
 *
 * @opensearch.api
 * */
@PublicApi(since = "1.0.0")
public class BigArrays {

    /**
     * The NON_RECYCLING_INSTANCE constant.
     */
    public static final BigArrays NON_RECYCLING_INSTANCE = new BigArrays(null, null, CircuitBreaker.REQUEST);

    /** Return the next size to grow to that is &gt;= <code>minTargetSize</code>.
      * Inspired from {@link ArrayUtil#oversize(int, int)} and adapted to play nicely with paging.
     *
     * @param minTargetSize the min target size
     * @param pageSize the page size
     * @param bytesPerElement the bytes per element
     * @return the over size
      */
    public static long overSize(long minTargetSize, int pageSize, int bytesPerElement) {
        if (minTargetSize < 0) {
            throw new IllegalArgumentException("minTargetSize must be >= 0");
        }
        if (pageSize < 0) {
            throw new IllegalArgumentException("pageSize must be > 0");
        }
        if (bytesPerElement <= 0) {
            throw new IllegalArgumentException("bytesPerElement must be > 0");
        }

        long newSize;
        if (minTargetSize < pageSize) {
            newSize = Math.min(ArrayUtil.oversize((int) minTargetSize, bytesPerElement), pageSize);
        } else {
            final long pages = (minTargetSize + pageSize - 1) / pageSize; // ceil(minTargetSize/pageSize)
            newSize = pages * pageSize;
        }

        return newSize;
    }

    static boolean indexIsInt(long index) {
        return index == (int) index;
    }

    /**
     * Base array wrapper class
     *
     * @opensearch.internal
     */
    private abstract static class AbstractArrayWrapper extends AbstractArray implements BigArray {

        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ByteArrayWrapper.class);

        private final Releasable releasable;
        private final long size;

        AbstractArrayWrapper(BigArrays bigArrays, long size, Releasable releasable, boolean clearOnResize) {
            super(bigArrays, clearOnResize);
            this.releasable = releasable;
            this.size = size;
        }

        @Override
        public final long size() {
            return size;
        }

        @Override
        protected final void doClose() {
            Releasables.close(releasable);
        }

    }

    /**
     * Wraps a byte array
     *
     * @opensearch.internal
     */
    private static class ByteArrayWrapper extends AbstractArrayWrapper implements ByteArray {

        private final byte[] array;

        ByteArrayWrapper(BigArrays bigArrays, byte[] array, long size, Recycler.V<byte[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public byte get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public byte set(long index, byte value) {
            assert indexIsInt(index);
            final byte ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public boolean get(long index, int len, BytesRef ref) {
            assert indexIsInt(index);
            ref.bytes = array;
            ref.offset = (int) index;
            ref.length = len;
            return false;
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert indexIsInt(index);
            System.arraycopy(buf, offset, array, (int) index, len);
        }

        @Override
        public void fill(long fromIndex, long toIndex, byte value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }

        @Override
        public boolean hasArray() {
            return true;
        }

        @Override
        public byte[] array() {
            return array;
        }
    }

    final PageCacheRecycler recycler;
    private final CircuitBreakerService breakerService;
    private final boolean checkBreaker;
    private final BigArrays circuitBreakingInstance;
    private final String breakerName;

    /**
     * Creates a new BigArrays.
     *
     * @param recycler the recycler
     * @param breakerService the breaker service
     * @param breakerName the breaker name
     */
    public BigArrays(PageCacheRecycler recycler, @Nullable final CircuitBreakerService breakerService, String breakerName) {
        // Checking the breaker is disabled if not specified
        this(recycler, breakerService, breakerName, false);
    }

    /**
     * Creates a new BigArrays.
     *
     * @param recycler the recycler
     * @param breakerService the breaker service
     * @param breakerName the breaker name
     * @param checkBreaker the check breaker
     */
    protected BigArrays(
        PageCacheRecycler recycler,
        @Nullable final CircuitBreakerService breakerService,
        String breakerName,
        boolean checkBreaker
    ) {
        this.checkBreaker = checkBreaker;
        this.recycler = recycler;
        this.breakerService = breakerService;
        this.breakerName = breakerName;
        if (checkBreaker) {
            this.circuitBreakingInstance = this;
        } else {
            this.circuitBreakingInstance = new BigArrays(recycler, breakerService, breakerName, true);
        }
    }

    /**
     * Adjust the circuit breaker with the given delta, if the delta is
     * negative, or checkBreaker is false, the breaker will be adjusted
     * without tripping.  If the data was already created before calling
     * this method, and the breaker trips, we add the delta without breaking
     * to account for the created data.  If the data has not been created yet,
     * we do not add the delta to the breaker if it trips.
     */
    void adjustBreaker(final long delta, final boolean isDataAlreadyCreated) {
        if (this.breakerService != null) {
            CircuitBreaker breaker = this.breakerService.getBreaker(breakerName);
            if (this.checkBreaker) {
                // checking breaker means potentially tripping, but it doesn't
                // have to if the delta is negative
                if (delta > 0) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(delta, "<reused_arrays>");
                    } catch (CircuitBreakingException e) {
                        if (isDataAlreadyCreated) {
                            // since we've already created the data, we need to
                            // add it so closing the stream re-adjusts properly
                            breaker.addWithoutBreaking(delta);
                        }
                        // re-throw the original exception
                        throw e;
                    }
                } else {
                    breaker.addWithoutBreaking(delta);
                }
            } else {
                // even if we are not checking the breaker, we need to adjust
                // its' totals, so add without breaking
                breaker.addWithoutBreaking(delta);
            }
        }
    }

    private <T extends AbstractBigArray> T resizeInPlace(T array, long newSize) {
        final long oldMemSize = array.ramBytesUsed();
        final long oldSize = array.size();
        assert oldMemSize == array.ramBytesEstimated(oldSize)
            : "ram bytes used should equal that which was previously estimated: ramBytesUsed="
                + oldMemSize
                + ", ramBytesEstimated="
                + array.ramBytesEstimated(oldSize);
        final long estimatedIncreaseInBytes = array.ramBytesEstimated(newSize) - oldMemSize;
        adjustBreaker(estimatedIncreaseInBytes, false);
        array.resize(newSize);
        return array;
    }

    private <T extends BigArray> T validate(T array) {
        boolean success = false;
        try {
            adjustBreaker(array.ramBytesUsed(), true);
            success = true;
        } finally {
            if (!success) {
                Releasables.closeWhileHandlingException(array);
            }
        }
        return array;
    }

    /**
     * Allocate a new {@link ByteArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     * @return the new byte array
     */
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        if (size > PageCacheRecycler.BYTE_PAGE_SIZE) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigByteArray.estimateRamBytes(size), false);
            return new BigByteArray(size, this, clearOnResize);
        } else if (size >= PageCacheRecycler.BYTE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<byte[]> page = recycler.bytePage(clearOnResize);
            return validate(new ByteArrayWrapper(this, page.v(), size, page, clearOnResize));
        } else {
            return validate(new ByteArrayWrapper(this, new byte[(int) size], size, null, clearOnResize));
        }
    }

    /**
     * Resize the array to the exact provided size.
     *
     * @param array the array
     * @param size the size
     * @return this instance
     */
    public ByteArray resize(ByteArray array, long size) {
        if (array instanceof BigByteArray) {
            return resizeInPlace((BigByteArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final ByteArray newArray = newByteArray(size, arr.clearOnResize);
            final byte[] rawArray = ((ByteArrayWrapper) array).array;
            newArray.set(0, rawArray, 0, (int) Math.min(rawArray.length, newArray.size()));
            arr.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>,
      * preserving content, and potentially reusing part of the provided array.
     *
     * @param array the array
     * @param minSize the min size
     * @return this instance
      */
    public ByteArray grow(ByteArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.BYTE_PAGE_SIZE, 1);
        return resize(array, newSize);
    }
}
