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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An iterator over a fixed array of nodes that starts from a round-robin
 * position and visits each node exactly once, wrapping around the array.
 */
public class NodeIterator implements Iterator<Node> {

    /** A shared counter used to determine the round-robin start position. */
    protected static AtomicInteger positionCounter = new AtomicInteger();

    /** The nodes to iterate over. */
    protected final Node[] nodes;

    /** The current position in the node array. */
    protected int position;

    /** The number of nodes returned so far. */
    protected int count = 0;

    /**
     * Creates a new iterator over the given nodes, starting at a round-robin position.
     *
     * @param nodes the nodes to iterate over
     */
    public NodeIterator(final Node[] nodes) {
        this.nodes = nodes;
        this.position = positionCounter.incrementAndGet() % nodes.length;
        if (this.position < 0) {
            this.position *= -1;
        }
    }

    @Override
    public boolean hasNext() {
        return count < nodes.length;
    }

    @Override
    public Node next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No node in this iterator.");
        }
        final Node node = nodes[position];
        position++;
        if (position >= nodes.length) {
            position = 0;
        }
        count++;
        return node;
    }

}
