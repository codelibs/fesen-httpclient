/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.codelibs.fesen.opensearch.common;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A convenient class which offers a semi-immutable object wrapper implementation which allows one
 * to set the value of an object exactly once, and retrieve it many times. If {@link #set(Object)}
 * is called more than once, {@link AlreadySetException} is thrown and the operation will fail.
 * <p>
 * This is borrowed from lucene's experimental API. It is not reused to eliminate the dependency
 * on lucene core for such a simple (standalone) utility class that may change beyond OpenSearch needs.
 *
 * @param <T> the element type
 * @opensearch.api
 */
public final class SetOnce<T> implements Cloneable {

    /** Thrown when {@link SetOnce#set(Object)} is called more than once. */
    public static final class AlreadySetException extends IllegalStateException {
        /**
         * Creates a new AlreadySetException.
         */
        public AlreadySetException() {
            super("The object cannot be set twice!");
        }
    }

    /** Holding object and marking that it was already set */
    private static final class Wrapper<T> {
        private T object;

        private Wrapper(T object) {
            this.object = object;
        }
    }

    private final AtomicReference<Wrapper<T>> set;

    /**
     * A default constructor which does not set the internal object, and allows setting it by calling
     * {@link #set(Object)}.
     */
    public SetOnce() {
        set = new AtomicReference<>();
    }

    /**
     * Sets the given object. If the object has already been set, an exception is thrown.
     *
     * @param obj the object to compare with
     */
    public final void set(T obj) {
        if (!trySet(obj)) {
            throw new AlreadySetException();
        }
    }

    /**
     * Sets the given object if none was set before.
     *
     * @param obj the object to compare with
     * @return true if object was set successfully, false otherwise
     */
    public final boolean trySet(T obj) {
        return set.compareAndSet(null, new Wrapper<>(obj));
    }

    /**
     * Returns the object set by {@link #set(Object)}.
     *
     * @return the value
     */
    public final T get() {
        Wrapper<T> wrapper = set.get();
        return wrapper == null ? null : wrapper.object;
    }
}
