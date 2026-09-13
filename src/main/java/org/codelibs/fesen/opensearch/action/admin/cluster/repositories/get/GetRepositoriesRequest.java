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

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Get repository request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetRepositoriesRequest extends ClusterManagerNodeReadRequest<GetRepositoriesRequest> {

    private String[] repositories = Strings.EMPTY_ARRAY;

    public GetRepositoriesRequest() {}

    /**
     * Constructs a new get repositories request with a list of repositories.
     * <p>
     * If the list of repositories is empty or it contains a single element "_all", all registered repositories
     * are returned.
     *
     * @param repositories list of repositories
     */
    public GetRepositoriesRequest(String[] repositories) {
        this.repositories = repositories;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(repositories);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositories == null) {
            validationException = addValidationError("repositories is null", validationException);
        }
        return validationException;
    }

    /**
     * The names of the repositories.
     *
     * @return  array of repository names
     */
    public String[] repositories() {
        return this.repositories;
    }
}
