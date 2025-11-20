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

import java.util.concurrent.atomic.AtomicBoolean;

public class Node {

    protected String host;

    protected AtomicBoolean available = new AtomicBoolean(true);

    public Node(final String host) {
        this.host = host;
    }

    public String getUrl(final String path) {
        return host + path;
    }

    public boolean isAvailable() {
        return available.get();
    }

    public void setAvailable(final boolean available) {
        this.available.set(available);
    }

    @Override
    public String toString() {
        return "[" + host + "][" + (available.get() ? "green" : "red") + "]";
    }
}
