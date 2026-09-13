/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.codelibs.fesen.opensearch.transport;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * The client-side remnant of the remote-cluster awareness base class: the naming convention that
 * qualifies an index with the cluster alias it came from. Connecting to remote clusters is a
 * node-side concern and is not carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RemoteClusterAware {

    /** The character that separates a cluster alias from an index name. */
    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';

    /** The cluster-alias key the local cluster's indices are grouped under. */
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    private RemoteClusterAware() {
    }

    /**
     * Qualifies an index name with the alias of the cluster it came from.
     *
     * @param clusterAlias the cluster alias, or {@code null} for the local cluster
     * @param indexName the index name
     * @return the qualified index name
     */
    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName
            : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
