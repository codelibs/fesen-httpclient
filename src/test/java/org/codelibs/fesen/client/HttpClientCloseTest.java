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
package org.codelibs.fesen.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

/**
 * Test class for HttpClient.close() method
 */
class HttpClientCloseTest {

    @Test
    void test_close_normalShutdown() {
        final Settings settings = Settings.builder().putList("http.hosts", "http://localhost:9200").build();

        final HttpClient client = new HttpClient(settings, null);
        assertFalse(client.threadPool.isShutdown());

        // Close should shutdown the thread pool gracefully
        client.close();

        assertTrue(client.threadPool.isShutdown());
    }

    @Test
    void test_close_multipleCalls() {
        final Settings settings = Settings.builder().putList("http.hosts", "http://localhost:9200").build();

        final HttpClient client = new HttpClient(settings, null);

        // First close
        client.close();
        assertTrue(client.threadPool.isShutdown());

        // Second close should not throw exception
        client.close();
        assertTrue(client.threadPool.isShutdown());
    }

    @Test
    void test_close_withCustomThreadPoolSize() {
        final Settings settings =
                Settings.builder().putList("http.hosts", "http://localhost:9200").put("thread_pool.http.size", 4).build();

        final HttpClient client = new HttpClient(settings, null);
        assertFalse(client.threadPool.isShutdown());

        client.close();

        assertTrue(client.threadPool.isShutdown());
    }

    @Test
    void test_close_withAsyncThreadPool() {
        final Settings settings =
                Settings.builder().putList("http.hosts", "http://localhost:9200").put("thread_pool.http.async", true).build();

        final HttpClient client = new HttpClient(settings, null);
        assertFalse(client.threadPool.isShutdown());

        client.close();

        assertTrue(client.threadPool.isShutdown());
    }

    @Test
    void test_close_interruptedThread() throws InterruptedException {
        final Settings settings = Settings.builder().putList("http.hosts", "http://localhost:9200").build();

        final HttpClient client = new HttpClient(settings, null);

        // Create a thread that will interrupt itself while closing
        final Thread thread = new Thread(() -> {
            // Interrupt the current thread before calling close
            Thread.currentThread().interrupt();

            // Close should handle the interrupt properly
            client.close();

            // Verify that interrupt status is preserved
            assertTrue(Thread.interrupted(), "Interrupt status should be preserved");
        });

        thread.start();
        thread.join(5000); // Wait up to 5 seconds

        assertFalse(thread.isAlive(), "Thread should have completed");
        assertTrue(client.threadPool.isShutdown());
    }

    @Test
    void test_close_nodeManagerAlreadyClosed() {
        final Settings settings = Settings.builder().putList("http.hosts", "http://localhost:9200").build();

        final HttpClient client = new HttpClient(settings, null);

        // Close node manager first
        client.nodeManager.close();

        // Close should not throw exception even if node manager is already closed
        client.close();

        assertTrue(client.threadPool.isShutdown());
    }
}
