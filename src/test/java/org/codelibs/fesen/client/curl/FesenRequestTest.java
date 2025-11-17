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
package org.codelibs.fesen.client.curl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Optional;

import org.codelibs.curl.Curl;
import org.codelibs.fesen.client.node.Node;
import org.codelibs.fesen.client.node.NodeManager;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IndexNotFoundException;

class FesenRequestTest {

    @Test
    void test_getNode_withAvailableNodes() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200", "http://server2:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        final Optional<Node> node1 = request.getNode();
        assertTrue(node1.isPresent());

        final Optional<Node> node2 = request.getNode();
        assertTrue(node2.isPresent());

        // Different nodes should be returned (round-robin)
        assertFalse(node1.get().toString().equals(node2.get().toString()));
    }

    @Test
    void test_getNode_withAllNodesUnavailable() throws Exception {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200", "http://server2:9200" }, node -> null);

        // Use reflection to access protected nodes field
        final Field nodesField = NodeManager.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        final Node[] nodes = (Node[]) nodesField.get(nodeManager);

        // Mark all nodes as unavailable
        nodes[0].setAvailable(false);
        nodes[1].setAvailable(false);

        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // getNode() should reset all nodes to available and return one
        final Optional<Node> node = request.getNode();
        assertTrue(node.isPresent());
    }

    @Test
    void test_getNode_iteratesWithoutRecursion() throws Exception {
        // Test with many nodes to ensure iteration works without stack overflow
        final String[] hosts = new String[100];
        for (int i = 0; i < 100; i++) {
            hosts[i] = "http://server" + i + ":9200";
        }
        final NodeManager nodeManager = new NodeManager(hosts, node -> null);

        // Use reflection to access protected nodes field
        final Field nodesField = NodeManager.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        final Node[] nodes = (Node[]) nodesField.get(nodeManager);

        // Mark first 99 nodes as unavailable
        for (int i = 0; i < 99; i++) {
            nodes[i].setAvailable(false);
        }

        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // Should find the 100th node without stack overflow
        final Optional<Node> node = request.getNode();
        assertTrue(node.isPresent());
        assertTrue(node.get().toString().contains("server99"));
    }

    @Test
    void test_isTargetException_withIndexNotFoundException() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // IndexNotFoundException should not be a target for retry
        final IndexNotFoundException exception = new IndexNotFoundException("test_index");
        assertFalse(request.isTargetException(exception));
    }

    @Test
    void test_isTargetException_withOtherExceptions() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // Other exceptions should be target for retry
        assertTrue(request.isTargetException(new RuntimeException("Connection failed")));
        assertTrue(request.isTargetException(new IllegalStateException("Server error")));
    }

    @Test
    void test_isTargetException_withWrappedIndexNotFoundException() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // Wrapped IndexNotFoundException should not be a target for retry
        final Exception wrapped = new RuntimeException("Wrapper", new IndexNotFoundException("test_index"));
        assertFalse(request.isTargetException(wrapped));
    }

    @Test
    void test_isTargetException_withDeeplyNestedIndexNotFoundException() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // Deeply nested IndexNotFoundException should not be a target for retry
        Exception nested = new IndexNotFoundException("test_index");
        for (int i = 0; i < 5; i++) {
            nested = new RuntimeException("Level " + i, nested);
        }
        assertFalse(request.isTargetException(nested));
    }

    @Test
    void test_isTargetException_withDeepNesting_stopsAt10Levels() {
        final NodeManager nodeManager = new NodeManager(new String[] { "http://server1:9200" }, node -> null);
        final FesenRequest request = new FesenRequest(Curl.get(null), nodeManager, "/test");

        // Create a chain of 15 exceptions with IndexNotFoundException at the bottom
        Exception nested = new IndexNotFoundException("test_index");
        for (int i = 0; i < 15; i++) {
            nested = new RuntimeException("Level " + i, nested);
        }

        // Should stop checking after 10 levels and return true (as it didn't find IndexNotFoundException)
        assertTrue(request.isTargetException(nested));
    }
}
