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

package org.codelibs.fesen.opensearch.common.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.recycler.AbstractRecyclerC;
import org.codelibs.fesen.opensearch.common.recycler.Recycler;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.codelibs.fesen.opensearch.core.common.bytes.PagedBytesReference;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.Locale;

import static org.codelibs.fesen.opensearch.common.recycler.Recyclers.concurrent;
import static org.codelibs.fesen.opensearch.common.recycler.Recyclers.concurrentDeque;
import static org.codelibs.fesen.opensearch.common.recycler.Recyclers.dequeFactory;
import static org.codelibs.fesen.opensearch.common.recycler.Recyclers.none;

/**
 * A recycler of fixed-size pages.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class PageCacheRecycler {

    /**
     * The TYPE_SETTING constant.
     */
    public static final Setting<Type> TYPE_SETTING = new Setting<>(
        "cache.recycler.page.type",
        Type.CONCURRENT.name(),
        Type::parse,
        Property.NodeScope
    );
    /**
     * The LIMIT_HEAP_SETTING constant.
     */
    public static final Setting<ByteSizeValue> LIMIT_HEAP_SETTING = Setting.memorySizeSetting(
        "cache.recycler.page.limit.heap",
        "10%",
        Property.NodeScope
    );
    /**
     * The WEIGHT_BYTES_SETTING constant.
     */
    public static final Setting<Double> WEIGHT_BYTES_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.bytes",
        1d,
        0d,
        Property.NodeScope
    );
    /**
     * The WEIGHT_LONG_SETTING constant.
     */
    public static final Setting<Double> WEIGHT_LONG_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.longs",
        1d,
        0d,
        Property.NodeScope
    );
    /**
     * The WEIGHT_INT_SETTING constant.
     */
    public static final Setting<Double> WEIGHT_INT_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.ints",
        1d,
        0d,
        Property.NodeScope
    );
    // object pages are less useful to us so we give them a lower weight by default
    /**
     * The WEIGHT_OBJECTS_SETTING constant.
     */
    public static final Setting<Double> WEIGHT_OBJECTS_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.objects",
        0.1d,
        0d,
        Property.NodeScope
    );

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = PagedBytesReference.PAGE_SIZE_IN_BYTES;
    /**
     * The OBJECT_PAGE_SIZE constant.
     */
    public static final int OBJECT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    /**
     * The LONG_PAGE_SIZE constant.
     */
    public static final int LONG_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Long.BYTES;
    /**
     * The INT_PAGE_SIZE constant.
     */
    public static final int INT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    /**
     * The BYTE_PAGE_SIZE constant.
     */
    public static final int BYTE_PAGE_SIZE = PAGE_SIZE_IN_BYTES;

    private final Recycler<byte[]> bytePage;
    private final Recycler<int[]> intPage;
    private final Recycler<long[]> longPage;
    private final Recycler<Object[]> objectPage;

    /**
     * The NON_RECYCLING_INSTANCE constant.
     */
    public static final PageCacheRecycler NON_RECYCLING_INSTANCE;

    static {
        NON_RECYCLING_INSTANCE = new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "0%").build());
    }

    /**
     * Creates a new PageCacheRecycler.
     *
     * @param settings the settings
     */
    public PageCacheRecycler(Settings settings) {
        final Type type = TYPE_SETTING.get(settings);
        final long limit = LIMIT_HEAP_SETTING.get(settings).getBytes();
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);

        // We have a global amount of memory that we need to divide across data types.
        // Since some types are more useful than other ones we give them different weights.
        // Trying to store all of them in a single stack would be problematic because eg.
        // a work load could fill the recycler with only byte[] pages and then another
        // workload that would work with double[] pages couldn't recycle them because there
        // is no space left in the stack/queue. LRU/LFU policies are not an option either
        // because they would make obtain/release too costly: we really need constant-time
        // operations.
        // Ultimately a better solution would be to only store one kind of data and have the
        // ability to interpret it either as a source of bytes, doubles, longs, etc. eg. thanks
        // to direct ByteBuffers or sun.misc.Unsafe on a byte[] but this would have other issues
        // that would need to be addressed such as garbage collection of native memory or safety
        // of Unsafe writes.
        final double bytesWeight = WEIGHT_BYTES_SETTING.get(settings);
        final double intsWeight = WEIGHT_INT_SETTING.get(settings);
        final double longsWeight = WEIGHT_LONG_SETTING.get(settings);
        final double objectsWeight = WEIGHT_OBJECTS_SETTING.get(settings);

        final double totalWeight = bytesWeight + intsWeight + longsWeight + objectsWeight;
        final int maxPageCount = (int) Math.min(Integer.MAX_VALUE, limit / PAGE_SIZE_IN_BYTES);

        final int maxBytePageCount = (int) (bytesWeight * maxPageCount / totalWeight);
        bytePage = build(type, maxBytePageCount, allocatedProcessors, new AbstractRecyclerC<byte[]>() {
            @Override
            public byte[] newInstance() {
                return new byte[BYTE_PAGE_SIZE];
            }

            @Override
            public void recycle(byte[] value) {
                // nothing to do
            }
        });

        final int maxIntPageCount = (int) (intsWeight * maxPageCount / totalWeight);
        intPage = build(type, maxIntPageCount, allocatedProcessors, new AbstractRecyclerC<int[]>() {
            @Override
            public int[] newInstance() {
                return new int[INT_PAGE_SIZE];
            }

            @Override
            public void recycle(int[] value) {
                // nothing to do
            }
        });

        final int maxLongPageCount = (int) (longsWeight * maxPageCount / totalWeight);
        longPage = build(type, maxLongPageCount, allocatedProcessors, new AbstractRecyclerC<long[]>() {
            @Override
            public long[] newInstance() {
                return new long[LONG_PAGE_SIZE];
            }

            @Override
            public void recycle(long[] value) {
                // nothing to do
            }
        });

        final int maxObjectPageCount = (int) (objectsWeight * maxPageCount / totalWeight);
        objectPage = build(type, maxObjectPageCount, allocatedProcessors, new AbstractRecyclerC<Object[]>() {
            @Override
            public Object[] newInstance() {
                return new Object[OBJECT_PAGE_SIZE];
            }

            @Override
            public void recycle(Object[] value) {
                Arrays.fill(value, null); // we need to remove the strong refs on the objects stored in the array
            }
        });

        assert PAGE_SIZE_IN_BYTES * (maxBytePageCount + maxIntPageCount + maxLongPageCount + maxObjectPageCount) <= limit;
    }

    /**
     * Returns the byte page.
     *
     * @param clear the clear
     * @return the byte page
     */
    public Recycler.V<byte[]> bytePage(boolean clear) {
        final Recycler.V<byte[]> v = bytePage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), (byte) 0);
        }
        return v;
    }

    /**
     * Returns the int page.
     *
     * @param clear the clear
     * @return the int page
     */
    public Recycler.V<int[]> intPage(boolean clear) {
        final Recycler.V<int[]> v = intPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0);
        }
        return v;
    }

    /**
     * Returns the long page.
     *
     * @param clear the clear
     * @return the long page
     */
    public Recycler.V<long[]> longPage(boolean clear) {
        final Recycler.V<long[]> v = longPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0L);
        }
        return v;
    }

    /**
     * Returns the object page.
     *
     * @return the object page
     */
    public Recycler.V<Object[]> objectPage() {
        // object pages are cleared on release anyway
        return objectPage.obtain();
    }

    private static <T> Recycler<T> build(Type type, int limit, int availableProcessors, Recycler.C<T> c) {
        final Recycler<T> recycler;
        if (limit == 0) {
            recycler = none(c);
        } else {
            recycler = type.build(c, limit, availableProcessors);
        }
        return recycler;
    }

    /**
     * Type of the page cache recycler
     *
     * @opensearch.internal
     */
    public enum Type {
        /**
         * The QUEUE value.
         */
        QUEUE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrentDeque(c, limit);
            }
        },
        /**
         * The CONCURRENT value.
         */
        CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrent(dequeFactory(c, limit / availableProcessors), availableProcessors);
            }
        },
        /**
         * The NONE value.
         */
        NONE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return none(c);
            }
        };

        /**
         * Parses this instance.
         *
         * @param type the type
         * @return this instance
         */
        public static Type parse(String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("no type support [" + type + "]");
            }
        }

        abstract <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors);
    }
}
