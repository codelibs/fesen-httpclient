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
package org.codelibs.fesen.opensearch.transport;

/**
 * The client-side remnant of the transport thread helpers: the assertions other code makes about
 * not blocking a transport thread. A client has no transport threads, so the checks are trivially
 * satisfied.
 *
 * @opensearch.internal
 */
public final class Transports {

    /** The thread-name prefix a mock transport uses in tests. */
    public static final String TEST_MOCK_TRANSPORT_THREAD_PREFIX = "__mock_network_thread";

    private Transports() {
    }

    /**
     * Returns whether the given thread is a transport worker. A client never has one.
     *
     * @param t the thread to test
     * @return {@code true} if the thread is a transport worker
     */
    public static boolean isTransportThread(Thread t) {
        return t.getName().contains(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
    }

    /**
     * Asserts that the current thread is not a transport worker.
     *
     * @param reason why the caller must not be on a transport thread
     * @return always {@code true}
     */
    public static boolean assertNotTransportThread(String reason) {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) == false : "Expected current thread [" + t + "] to not be a transport thread. Reason: [" + reason + "]";
        return true;
    }
}
