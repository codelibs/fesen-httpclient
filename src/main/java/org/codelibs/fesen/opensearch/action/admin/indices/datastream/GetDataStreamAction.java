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

package org.codelibs.fesen.opensearch.action.admin.indices.datastream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.health.ClusterHealthStatus;
import org.codelibs.fesen.opensearch.cluster.health.ClusterStateHealth;
import org.codelibs.fesen.opensearch.cluster.metadata.DataStream;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action for getting a datastream
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetDataStreamAction extends ActionType<GetDataStreamAction.Response> {

    public static final GetDataStreamAction INSTANCE = new GetDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/get";

    private GetDataStreamAction() {
        super(NAME, Response::new);
    }

    /**
     * Request for getting data streams
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Request extends ClusterManagerNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private String[] names;

        public Request(String[] names) {
            this.names = names;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readOptionalStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            // this doesn't really matter since data stream name resolution isn't affected by IndicesOptions and
            // a data stream's backing indices are retrieved from its metadata
            return IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }
    }

    /**
     * Response for getting data streams
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField DATASTREAMS_FIELD = new ParseField("data_streams");

        /**
         * Data streams information
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        public static class DataStreamInfo extends AbstractDiffable<DataStreamInfo> implements ToXContentObject {

            public static final ParseField STATUS_FIELD = new ParseField("status");
            public static final ParseField INDEX_TEMPLATE_FIELD = new ParseField("template");

            DataStream dataStream;
            ClusterHealthStatus dataStreamStatus;
            @Nullable
            String indexTemplate;

            public DataStreamInfo(DataStream dataStream, ClusterHealthStatus dataStreamStatus, @Nullable String indexTemplate) {
                this.dataStream = dataStream;
                this.dataStreamStatus = dataStreamStatus;
                this.indexTemplate = indexTemplate;
            }

            public DataStreamInfo(StreamInput in) throws IOException {
                this(new DataStream(in), ClusterHealthStatus.readFrom(in), in.readOptionalString());
            }

            public DataStream getDataStream() {
                return dataStream;
            }

            public ClusterHealthStatus getDataStreamStatus() {
                return dataStreamStatus;
            }

            @Nullable
            public String getIndexTemplate() {
                return indexTemplate;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                dataStream.writeTo(out);
                dataStreamStatus.writeTo(out);
                out.writeOptionalString(indexTemplate);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(DataStream.NAME_FIELD.getPreferredName(), dataStream.getName());
                builder.field(DataStream.TIMESTAMP_FIELD_FIELD.getPreferredName(), dataStream.getTimeStampField());
                builder.field(DataStream.INDICES_FIELD.getPreferredName(), dataStream.getIndices());
                builder.field(DataStream.GENERATION_FIELD.getPreferredName(), dataStream.getGeneration());
                builder.field(STATUS_FIELD.getPreferredName(), dataStreamStatus);
                if (indexTemplate != null) {
                    builder.field(INDEX_TEMPLATE_FIELD.getPreferredName(), indexTemplate);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                DataStreamInfo that = (DataStreamInfo) o;
                return dataStream.equals(that.dataStream)
                    && dataStreamStatus == that.dataStreamStatus
                    && Objects.equals(indexTemplate, that.indexTemplate);
            }

            @Override
            public int hashCode() {
                return Objects.hash(dataStream, dataStreamStatus, indexTemplate);
            }
        }

        private final List<DataStreamInfo> dataStreams;

        public Response(List<DataStreamInfo> dataStreams) {
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readList(DataStreamInfo::new));
        }

        public List<DataStreamInfo> getDataStreams() {
            return dataStreams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(dataStreams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(DATASTREAMS_FIELD.getPreferredName());
            for (DataStreamInfo dataStream : dataStreams) {
                dataStream.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return dataStreams.equals(response.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreams);
        }
    }

    /**
     * Transport Action for getting data streams
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeReadAction<Request, Response>
        implements
            TransportIndicesResolvingAction<Request> {

        private static final Logger logger = LogManager.getLogger(TransportAction.class);

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            List<DataStream> dataStreams = getDataStreams(state, indexNameExpressionResolver, request);
            List<Response.DataStreamInfo> dataStreamInfos = new ArrayList<>(dataStreams.size());
            for (DataStream dataStream : dataStreams) {
                String indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dataStream.getName(), false);
                String ilmPolicyName = null;
                if (indexTemplate != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
                    ilmPolicyName = settings.get("index.lifecycle.name");
                } else {
                    logger.warn(
                        "couldn't find any matching template for data stream [{}]. has it been restored (and possibly renamed)"
                            + "from a snapshot?",
                        dataStream.getName()
                    );
                }
                ClusterStateHealth streamHealth = new ClusterStateHealth(
                    state,
                    dataStream.getIndices().stream().map(Index::getName).toArray(String[]::new)
                );
                dataStreamInfos.add(new Response.DataStreamInfo(dataStream, streamHealth.getStatus(), indexTemplate));
            }
            listener.onResponse(new Response(dataStreamInfos));
        }

        static List<DataStream> getDataStreams(ClusterState clusterState, IndexNameExpressionResolver iner, Request request) {
            List<String> results = iner.dataStreamNames(clusterState, request.indicesOptions(), request.names);
            Map<String, DataStream> dataStreams = clusterState.metadata().dataStreams();

            return results.stream().map(dataStreams::get).sorted(Comparator.comparing(DataStream::getName)).collect(Collectors.toList());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        public ResolvedIndices resolveIndices(Request request) {
            return ResolvedIndices.of(
                getDataStreams(clusterService.state(), indexNameExpressionResolver, request).stream()
                    .map(DataStream::getName)
                    .collect(Collectors.toList())
            );
        }
    }

}
