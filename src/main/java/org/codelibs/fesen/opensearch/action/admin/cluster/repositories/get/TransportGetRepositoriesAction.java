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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.Metadata;
import org.codelibs.fesen.opensearch.cluster.metadata.RepositoriesMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.RepositoryMetadata;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.repositories.RepositoryMissingException;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transport action for get repositories operation
 *
 * @opensearch.internal
 */
public class TransportGetRepositoriesAction extends TransportClusterManagerNodeReadAction<GetRepositoriesRequest, GetRepositoriesResponse> {

    @Inject
    public TransportGetRepositoriesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetRepositoriesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetRepositoriesRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetRepositoriesResponse read(StreamInput in) throws IOException {
        return new GetRepositoriesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetRepositoriesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        final GetRepositoriesRequest request,
        ClusterState state,
        final ActionListener<GetRepositoriesResponse> listener
    ) {
        Metadata metadata = state.metadata();
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (request.repositories().length == 0 || (request.repositories().length == 1 && "_all".equals(request.repositories()[0]))) {
            if (repositories != null) {
                listener.onResponse(new GetRepositoriesResponse(repositories));
            } else {
                listener.onResponse(new GetRepositoriesResponse(new RepositoriesMetadata(Collections.emptyList())));
            }
        } else {
            if (repositories != null) {
                Set<String> repositoriesToGet = new LinkedHashSet<>(); // to keep insertion order
                for (String repositoryOrPattern : request.repositories()) {
                    if (Regex.isSimpleMatchPattern(repositoryOrPattern) == false) {
                        repositoriesToGet.add(repositoryOrPattern);
                    } else {
                        for (RepositoryMetadata repository : repositories.repositories()) {
                            if (Regex.simpleMatch(repositoryOrPattern, repository.name())) {
                                repositoriesToGet.add(repository.name());
                            }
                        }
                    }
                }
                List<RepositoryMetadata> repositoryListBuilder = new ArrayList<>();
                for (String repository : repositoriesToGet) {
                    RepositoryMetadata repositoryMetadata = repositories.repository(repository);
                    if (repositoryMetadata == null) {
                        listener.onFailure(new RepositoryMissingException(repository));
                        return;
                    }
                    repositoryListBuilder.add(repositoryMetadata);
                }
                listener.onResponse(new GetRepositoriesResponse(new RepositoriesMetadata(repositoryListBuilder)));
            } else {
                listener.onFailure(new RepositoryMissingException(request.repositories()[0]));
            }
        }
    }
}
