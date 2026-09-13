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

package org.codelibs.fesen.opensearch.common.util.set;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * OpenSearch sets.
 *
 * @opensearch.internal
 */
public final class Sets {
    private Sets() {}

    public static <T> HashSet<T> newHashSet(T... elements) {
        Objects.requireNonNull(elements);
        HashSet<T> set = new HashSet<>(elements.length);
        Collections.addAll(set, elements);
        return set;
    }

    /**
     * A sorted set collector
     *
     * @opensearch.internal
     */
    private static class SortedSetCollector<T> implements Collector<T, SortedSet<T>, SortedSet<T>> {

        @Override
        public Supplier<SortedSet<T>> supplier() {
            return TreeSet::new;
        }

        @Override
        public BiConsumer<SortedSet<T>, T> accumulator() {
            return (s, e) -> s.add(e);
        }

        @Override
        public BinaryOperator<SortedSet<T>> combiner() {
            return (s, t) -> {
                s.addAll(t);
                return s;
            };
        }

        @Override
        public Function<SortedSet<T>, SortedSet<T>> finisher() {
            return Function.identity();
        }

        static final Set<Characteristics> CHARACTERISTICS = Collections.unmodifiableSet(
            EnumSet.of(Collector.Characteristics.IDENTITY_FINISH)
        );

        @Override
        public Set<Characteristics> characteristics() {
            return CHARACTERISTICS;
        }

    }

    public static <T> Set<T> union(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        Set<T> union = new HashSet<>(left);
        union.addAll(right);
        return union;
    }
}
