/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class NodeManagerTest {
    @Test
    void test_toNodeString_1() {
        NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        assertEquals("[server1:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_toNodeString_2() {
        NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200" });
        assertEquals("[server1:9200][green],[server2:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_toNodeString_3() {
        NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200", "server3:9200" });
        assertEquals("[server1:9200][green],[server2:9200][green],[server3:9200][green]", nodeManager.toNodeString());
    }

    @Test
    void test_getNodeIterator_1() {
        NodeManager nodeManager = new NodeManager(new String[] { "server1:9200" });
        NodeIterator nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        Node node1 = nodeIterator.next();
        assertEquals("[server1:9200][green]", node1.toString());
        assertFalse(nodeIterator.hasNext());

        nodeIterator = nodeManager.getNodeIterator();
        assertTrue(nodeIterator.hasNext());
        Node node2 = nodeIterator.next();
        assertEquals("[server1:9200][green]", node2.toString());
        assertFalse(nodeIterator.hasNext());
    }

    @Test
    void test_getNodeIterator_3() {
        NodeManager nodeManager = new NodeManager(new String[] { "server1:9200", "server2:9200", "server3:9200" });
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

}
