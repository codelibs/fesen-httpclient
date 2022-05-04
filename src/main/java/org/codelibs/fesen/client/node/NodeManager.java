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

import java.util.Arrays;
import java.util.stream.Collectors;

public class NodeManager {

    protected final Node[] nodes;

    public NodeManager(final String[] hosts) {
        this.nodes = new Node[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            this.nodes[i] = new Node(hosts[i]);
        }
    }

    public NodeIterator getNodeIterator() {
        return new NodeIterator(nodes);
    }

    public String toNodeString() {
        return Arrays.stream(nodes).map(Node::toString).collect(Collectors.joining(","));
    }

    public void setHeartbeatInterval(final long interval) {
        for (final Node node : nodes) {
            node.setHeartbeatInterval(interval);
        }
    }
}
