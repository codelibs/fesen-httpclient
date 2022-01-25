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

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;

public class HttpBulkAction extends HttpAction {

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
                final BulkResponse bulkResponse = BulkResponse.fromXContent(parser);
                listener.onResponse(bulkResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
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
