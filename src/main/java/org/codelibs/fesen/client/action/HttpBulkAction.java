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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.opensearch.common.xcontent.XContentParserUtils.throwUnknownToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestStatus;

public class HttpBulkAction extends HttpAction {

    private static final String ITEMS = "items";
    private static final String ERRORS = "errors";
    private static final String TOOK = "took";
    private static final String INGEST_TOOK = "ingest_took";
    private static final String STATUS = "status";
    private static final String ERROR = "error";

    protected final BulkAction action;

    public HttpBulkAction(final HttpClient client, final BulkAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        // http://ndjson.org/
        final StringBuilder buf = new StringBuilder(10000);
        try {
            final List<DocWriteRequest<?>> bulkRequests = request.requests();
            for (@SuppressWarnings("rawtypes")
            final DocWriteRequest req : bulkRequests) {
                buf.append(getStringfromDocWriteRequest(req));
                buf.append('\n');
                switch (req.opType().getId()) {
                case 0: { // INDEX
                    buf.append(XContentHelper.convertToJson(((IndexRequest) req).source(), false, XContentType.JSON));
                    buf.append('\n');
                    break;
                }
                case 1: { // CREATE
                    buf.append(XContentHelper.convertToJson(((IndexRequest) req).source(), false, XContentType.JSON));
                    buf.append('\n');
                    break;
                }
                case 2: { // UPDATE
                    try (final XContentBuilder builder =
                            ((UpdateRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
                        builder.flush();
                        buf.append(BytesReference.bytes(builder).utf8ToString());
                        buf.append('\n');
                    }
                    break;
                }
                case 3: { // DELETE
                    break;
                }
                default:
                    break;
                }
            }
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(buf.toString()).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final BulkResponse bulkResponse = fromXContent(parser);
                listener.onResponse(bulkResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    // BulkResponse.fromXContent(parser)
    protected BulkResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        long took = -1L;
        long ingestTook = BulkResponse.NO_INGEST_TOOK;
        List<BulkItemResponse> items = new ArrayList<>();

        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TOOK.equals(currentFieldName)) {
                    took = parser.longValue();
                } else if (INGEST_TOOK.equals(currentFieldName)) {
                    ingestTook = parser.longValue();
                } else if (ERRORS.equals(currentFieldName) == false) {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (ITEMS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        items.add(fromXContent(parser, items.size()));
                    }
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        return new BulkResponse(items.toArray(new BulkItemResponse[items.size()]), took, ingestTook);
    }

    // BulkItemResponse.fromXContent(parser, items.size()))
    protected BulkItemResponse fromXContent(XContentParser parser, int id) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        final OpType opType = OpType.fromString(currentFieldName);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        DocWriteResponse.Builder builder = null;
        CheckedConsumer<XContentParser, IOException> itemParser = null;

        if (opType == OpType.INDEX || opType == OpType.CREATE) {
            final IndexResponse.Builder indexResponseBuilder = new IndexResponse.Builder();
            builder = indexResponseBuilder;
            itemParser = (indexParser) -> IndexResponse.parseXContentFields(indexParser, indexResponseBuilder);

        } else if (opType == OpType.UPDATE) {
            final UpdateResponse.Builder updateResponseBuilder = new UpdateResponse.Builder();
            builder = updateResponseBuilder;
            itemParser = (updateParser) -> UpdateResponse.parseXContentFields(updateParser, updateResponseBuilder);

        } else if (opType == OpType.DELETE) {
            final DeleteResponse.Builder deleteResponseBuilder = new DeleteResponse.Builder();
            builder = deleteResponseBuilder;
            itemParser = (deleteParser) -> DeleteResponse.parseXContentFields(deleteParser, deleteResponseBuilder);
        } else {
            throwUnknownField(currentFieldName, parser.getTokenLocation());
        }

        RestStatus status = null;
        OpenSearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }

            if (ERROR.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    exception = OpenSearchException.fromXContent(parser);
                }
            } else if (STATUS.equals(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                itemParser.accept(parser);
            }
        }

        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);

        if (client.getEngineInfo().getType() == EngineType.ELASTICSEARCH8) {
            builder.setType("_doc");
        }

        BulkItemResponse bulkItemResponse;
        if (exception != null) {
            Failure failure = new Failure(builder.getShardId().getIndexName(), builder.getType(), builder.getId(), exception, status);
            bulkItemResponse = new BulkItemResponse(id, opType, failure);
        } else {
            bulkItemResponse = new BulkItemResponse(id, opType, builder.build());
        }
        return bulkItemResponse;
    }

    protected CurlRequest getCurlRequest(final BulkRequest request) {
        // RestBulkAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_bulk");
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        return curlRequest;
    }

    protected String getStringfromDocWriteRequest(final DocWriteRequest<?> request) {
        // BulkRequestParser
        final StringBuilder buf = new StringBuilder(100);
        final String opType = request.opType().getLowercase();
        buf.append("{\"").append(opType).append("\":{");
        appendStr(buf, "_index", request.index());
        if (request.type() != null &&
        // workaround fix for org.codelibs.fesen.action.index.IndexRequest.type()
                !request.type().equals("_doc")) {
            appendStr(buf.append(','), "_type", request.type());
        }
        if (request.id() != null) {
            appendStr(buf.append(','), "_id", request.id());
        }
        if (request.routing() != null) {
            appendStr(buf.append(','), "routing", request.routing());
        }
        //        if (request.opType() != null) {
        //            appendStr(buf.append(','), "op_type", opType);
        //        }
        if (request.version() >= 0) {
            appendStr(buf.append(','), "version", request.version());
        }
        if (VersionType.INTERNAL.equals(request.versionType())) {
            appendStr(buf.append(','), "version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            appendStr(buf.append(','), "if_seq_no", request.ifSeqNo());
        }
        if (request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            appendStr(buf.append(','), "if_primary_term", request.ifPrimaryTerm());
        }
        // retry_on_conflict
        switch (request.opType()) {
        case INDEX:
        case CREATE:
            final IndexRequest indexRequest = (IndexRequest) request;
            if (indexRequest.getPipeline() != null) {
                appendStr(buf.append(','), "pipeline", indexRequest.getPipeline());
            }
            break;
        case UPDATE:
            // final UpdateRequest updateRequest = (UpdateRequest) request;
            break;
        case DELETE:
            // final DeleteRequest deleteRequest = (DeleteRequest) request;
            break;
        default:
            break;
        }
        buf.append('}');
        buf.append('}');
        return buf.toString();
    }

    protected StringBuilder appendStr(final StringBuilder buf, final String key, final long value) {
        return buf.append('"').append(key).append("\":").append(value);
    }

    protected StringBuilder appendStr(final StringBuilder buf, final String key, final String value) {
        return buf.append('"').append(key).append("\":\"").append(value).append('"');
    }
}
