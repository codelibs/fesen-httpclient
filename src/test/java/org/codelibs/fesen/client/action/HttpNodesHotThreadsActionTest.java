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
package org.codelibs.fesen.client.action;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.opensearch.cluster.node.DiscoveryNode;

class HttpNodesHotThreadsActionTest {
    @Test
    void test_parseDiscoveryNode() {
        final HttpNodesHotThreadsAction action = new HttpNodesHotThreadsAction(null, NodesHotThreadsAction.INSTANCE);
        final DiscoveryNode discoveryNode = action.parseDiscoveryNode(
                "::: {685b6bd7cd44}{9a9zfqLYS6iTTNmrDsW0TQ}{OL3OXYBTQjSCQf6gJAgrHQ}{685b6bd7cd44}{172.17.0.5}{172.17.0.5:9300}{cdfhilmrstw}{ml.machine_memory=135007109120, ml.max_jvm_size=33285996544, xpack.installed=true}");
        assertEquals("685b6bd7cd44", discoveryNode.getName());
        assertEquals("9a9zfqLYS6iTTNmrDsW0TQ", discoveryNode.getId());
        assertEquals("OL3OXYBTQjSCQf6gJAgrHQ", discoveryNode.getEphemeralId());
        assertEquals("685b6bd7cd44", discoveryNode.getHostName());
        assertEquals("172.17.0.5", discoveryNode.getHostAddress());
    }
}
