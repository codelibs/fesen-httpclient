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

package org.codelibs.fesen.opensearch.rest.action.admin.indices;

import org.codelibs.fesen.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.codelibs.fesen.opensearch.cluster.metadata.ComponentTemplate;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.POST;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put component template
 *
 * @opensearch.api
 */
public class RestPutComponentTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutComponentTemplateAction.class);

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(POST, "/_component_template/{name}"), new Route(PUT, "/_component_template/{name}"));
    }

    @Override
    public String getName() {
        return "put_component_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        PutComponentTemplateAction.Request putRequest = new PutComponentTemplateAction.Request(request.param("name"));
        putRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", putRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(putRequest, request, deprecationLogger, getName());
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", "api"));
        putRequest.componentTemplate(ComponentTemplate.parse(request.contentParser()));

        return channel -> client.execute(PutComponentTemplateAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
