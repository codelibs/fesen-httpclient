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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.codelibs.curl.CurlException;
import org.junit.jupiter.api.Test;

class NodeManagerTest {
    @Test
    void test_toNodeString_1() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        assertEquals("[server1:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_toNodeString_2() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200" });
        assertEquals("[server1:9200][green],[server2:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_toNodeString_3() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200", "server3:9200" });
        assertEquals("[server1:9200][green],[server2:9200][green],[server3:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_getNodeIterator_1() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        NodeIterator nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        final Node node1 = nodeIterator.next();
        assertEquals("[server1:9200][green]", node1.toString());
        assertFalse(nodeIterator.hasNext());

        nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        final Node node2 = nodeIterator.next();
        assertEquals("[server1:9200][green]", node2.toString());
        assertFalse(nodeIterator.hasNext());
    }

    @Test
    void test_getNodeIterator_3() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200", "server3:9200" });
        NodeIterator nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        Node node1 = nodeIterator.next();
        assertTrue(nodeIterator.hasNext());
        Node node2 = nodeIterator.next();
        assertTrue(nodeIterator.hasNext());
        Node node3 = nodeIterator.next();
        assertFalse(nodeIterator.hasNext());
        assertNotEquals(node1.toString(), node2.toString());
        assertNotEquals(node1.toString(), node3.toString());
        assertNotEquals(node2.toString(), node3.toString());

        nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        node1 = nodeIterator.next();
        assertTrue(nodeIterator.hasNext());
        node2 = nodeIterator.next();
        assertTrue(nodeIterator.hasNext());
        node3 = nodeIterator.next();
        assertFalse(nodeIterator.hasNext());
        assertNotEquals(node1.toString(), node2.toString());
        assertNotEquals(node1.toString(), node3.toString());
        assertNotEquals(node2.toString(), node3.toString());

    }

    @Test
    void test_getCause() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        assertNull(nodeManager.getCause(null));
        assertEquals("aaa", nodeManager.getCause(new RuntimeException("aaa")).getMessage());
        assertEquals("aaa", nodeManager.getCause(new RuntimeException("aaa", new CurlException("bbb"))).getMessage());
        assertEquals("aaa", nodeManager.getCause(new CurlException("aaa")).getMessage());
        assertEquals("bbb", nodeManager.getCause(new CurlException("aaa", new RuntimeException("bbb"))).getMessage());
        assertEquals("bbb", nodeManager.getCause(new CurlException("aaa", new CurlException("bbb"))).getMessage());
        assertEquals("ccc",
                nodeManager.getCause(new CurlException("aaa", new CurlException("bbb", new RuntimeException("ccc")))).getMessage());
        assertEquals("ccc",
                nodeManager.getCause(new CurlException("aaa", new CurlException("bbb", new CurlException("ccc")))).getMessage());
    }

    @Test
    void test_close_withNullTimer() {
        // Create NodeManager without requestCreator (timer will be null)
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        // This should not throw NullPointerException
        nodeManager.close();
        // Verify that the manager is closed
        assertFalse(nodeManager.isRunning.get());
    }

    @Test
    void test_close_withTimer() throws InterruptedException {
        // Create NodeManager with requestCreator (timer will be initialized)
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" }, node -> null);
        assertTrue(nodeManager.isRunning.get());

        // Close the manager
        nodeManager.close();

        // Verify that the manager is closed
        assertFalse(nodeManager.isRunning.get());

        // Wait a bit to ensure timer is actually cancelled
        Thread.sleep(100);

        // Calling close again should not throw any exception
        nodeManager.close();
    }

    @Test
    void test_nodeIterator_allNodesUnavailable() {
        final NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200" });

        // Mark all nodes as unavailable
        final NodeIterator iter1 = nodeManager.getNodeIterator();
        while (iter1.hasNext()) {
            iter1.next().setAvailable(false);
        }

        // When getting new iterator, all nodes should be set back to available
        final NodeIterator iter2 = nodeManager.getNodeIterator();
        assertTrue(iter2.hasNext());
        final Node node1 = iter2.next();
        assertTrue(node1.isAvailable());
        assertTrue(iter2.hasNext());
        final Node node2 = iter2.next();
        assertTrue(node2.isAvailable());
    }
}
