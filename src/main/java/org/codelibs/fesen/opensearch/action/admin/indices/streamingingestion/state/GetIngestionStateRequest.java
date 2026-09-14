/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.pagination.PageParams;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;
import static org.codelibs.fesen.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;

/**
 * Request to get current ingestion state when using pull-based ingestion. This request supports retrieving index and
 * shard level state. By default, all shards of an index are included.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class GetIngestionStateRequest extends BroadcastRequest<GetIngestionStateRequest> {
    /**
     * The DEFAULT_PAGE_SIZE constant.
     */
    public static final int DEFAULT_PAGE_SIZE = 1000;
    /**
     * The DEFAULT_SORT_VALUE constant.
     */
    public static final String DEFAULT_SORT_VALUE = PARAM_ASC_SORT_VALUE;

    private int[] shards;
    private PageParams pageParams;

    // holds the <index,shard> pairs to consider when using pagination
    private List<IndexShardPair> indexShardPairsList;

    /**
     * Creates a new GetIngestionStateRequest.
     *
     * @param indices the indices
     */
    public GetIngestionStateRequest(String[] indices) {
        super();
        this.indices = indices;
        this.shards = new int[] {};
        this.pageParams = new PageParams(null, DEFAULT_SORT_VALUE, DEFAULT_PAGE_SIZE);
        this.indexShardPairsList = new ArrayList<>();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) {
            validationException = addValidationError("index is missing", validationException);
        } else if (indices.length != Arrays.stream(indices).collect(Collectors.toSet()).size()) {
            validationException = addValidationError("duplicate index names provided", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVIntArray(shards);
        out.writeOptionalWriteable(pageParams);
        out.writeList(indexShardPairsList);
    }

    /**
     * Returns the shards.
     *
     * @return the shards
     */
    public int[] getShards() {
        return shards;
    }

    private class IndexShardPair implements Writeable {
        String indexName;
        int shard;

        public IndexShardPair(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.shard = in.readVInt();
        }

        public IndexShardPair(String indexName, int shard) {
            this.indexName = indexName;
            this.shard = shard;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeVInt(shard);
        }
    }
}
