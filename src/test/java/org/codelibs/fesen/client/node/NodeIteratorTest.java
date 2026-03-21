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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.jupiter.api.Test;

class NodeIteratorTest {

    @Test
    void test_singleNode() {
        final Node[] nodes = { new Node("http://server1:9200") };
        final NodeIterator iter = new NodeIterator(nodes);
        assertTrue(iter.hasNext());
        final Node node = iter.next();
        assertEquals("[http://server1:9200][green]", node.toString());
        assertFalse(iter.hasNext());
    }

    @Test
    void test_multipleNodes_iteratesAll() {
        final Node[] nodes = { new Node("http://s1:9200"), new Node("http://s2:9200"), new Node("http://s3:9200") };
        final NodeIterator iter = new NodeIterator(nodes);
        final Set<String> visited = new HashSet<>();
        while (iter.hasNext()) {
            visited.add(iter.next().toString());
        }
        assertEquals(3, visited.size());
    }

    @Test
    void test_next_throwsNoSuchElementException() {
        final Node[] nodes = { new Node("http://server1:9200") };
        final NodeIterator iter = new NodeIterator(nodes);
        iter.next();
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    void test_roundRobin_distributesStartPosition() {
        final Node[] nodes = { new Node("http://s1:9200"), new Node("http://s2:9200"), new Node("http://s3:9200") };
        final Set<String> firstNodes = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            final NodeIterator iter = new NodeIterator(nodes);
            firstNodes.add(iter.next().toString());
        }
        // Over 10 iterations with 3 nodes, we should see more than 1 different starting node
        assertTrue(firstNodes.size() > 1);
    }

    @Test
    void test_hasNext_doesNotAdvance() {
        final Node[] nodes = { new Node("http://server1:9200") };
        final NodeIterator iter = new NodeIterator(nodes);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }

    @Test
    void test_wrapsAroundArrayBoundary() {
        final Node[] nodes = { new Node("http://s1:9200"), new Node("http://s2:9200") };
        // Create multiple iterators to hit different starting positions
        final Set<String> allVisited = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            final NodeIterator iter = new NodeIterator(nodes);
            while (iter.hasNext()) {
                allVisited.add(iter.next().toString());
            }
        }
        assertEquals(2, allVisited.size());
    }
}
