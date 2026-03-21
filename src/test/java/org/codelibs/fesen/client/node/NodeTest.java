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
package org.codelibs.fesen.client.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NodeTest {

    @Test
    void test_constructor() {
        final Node node = new Node("http://localhost:9200");
        assertTrue(node.isAvailable());
        assertEquals("[http://localhost:9200][green]", node.toString());
    }

    @Test
    void test_getUrl() {
        final Node node = new Node("http://localhost:9200");
        assertEquals("http://localhost:9200/_search", node.getUrl("/_search"));
        assertEquals("http://localhost:9200/", node.getUrl("/"));
        assertEquals("http://localhost:9200", node.getUrl(""));
    }

    @Test
    void test_isAvailable_defaultTrue() {
        final Node node = new Node("http://localhost:9200");
        assertTrue(node.isAvailable());
    }

    @Test
    void test_setAvailable() {
        final Node node = new Node("http://localhost:9200");
        assertTrue(node.isAvailable());

        node.setAvailable(false);
        assertFalse(node.isAvailable());

        node.setAvailable(true);
        assertTrue(node.isAvailable());
    }

    @Test
    void test_toString_available() {
        final Node node = new Node("http://server1:9200");
        assertEquals("[http://server1:9200][green]", node.toString());
    }

    @Test
    void test_toString_unavailable() {
        final Node node = new Node("http://server1:9200");
        node.setAvailable(false);
        assertEquals("[http://server1:9200][red]", node.toString());
    }

    @Test
    void test_concurrentAvailability() throws InterruptedException {
        final Node node = new Node("http://localhost:9200");
        final Thread t1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                node.setAvailable(false);
                node.setAvailable(true);
            }
        });
        final Thread t2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                node.isAvailable();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        // No exception means thread safety works
        assertTrue(true);
    }
}
