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

import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * An {@link AdminClient} wrapper that delegates to another admin client and wraps the indices
 * admin client with {@link HttpIndicesAdminClient}.
 */
public class HttpAdminClient implements AdminClient {

    private final AdminClient adminClient;

    /**
     * Creates a new admin client wrapping the given delegate.
     *
     * @param admin the admin client to delegate to
     */
    public HttpAdminClient(final AdminClient admin) {
        this.adminClient = admin;
    }

    @Override
    public ClusterAdminClient cluster() {
        return adminClient.cluster();
    }

    @Override
    public IndicesAdminClient indices() {
        return new HttpIndicesAdminClient(adminClient.indices());
    }

}
