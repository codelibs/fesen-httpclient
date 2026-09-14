/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.StatusToXContentObject;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastShardsHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Create point in time response with point in time id and shard success / failures
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class CreatePitResponse extends ActionResponse implements StatusToXContentObject {
    private static final ParseField ID = new ParseField("pit_id");
    private static final ParseField CREATION_TIME = new ParseField("creation_time");

    // point in time id
    private final String id;
    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;
    private final long creationTime;

    /**
     * Creates a new CreatePitResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public CreatePitResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        skippedShards = in.readVInt();
        creationTime = in.readLong();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = ShardSearchFailure.readShardSearchFailure(in);
            }
        }
    }

    /**
     * Creates a new CreatePitResponse.
     *
     * @param id the identifier
     * @param creationTime the creation time
     * @param totalShards the total shards
     * @param successfulShards the successful shards
     * @param skippedShards the skipped shards
     * @param failedShards the failed shards
     * @param shardFailures the shard failures
     */
    public CreatePitResponse(
        String id,
        long creationTime,
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        ShardSearchFailure[] shardFailures
    ) {
        this.id = id;
        this.creationTime = creationTime;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.failedShards = failedShards;
        this.shardFailures = shardFailures;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        BroadcastShardsHeader.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        builder.field(CREATION_TIME.getPreferredName(), creationTime);
        builder.endObject();
        return builder;
    }

    /**
     * Parse the create PIT response body into a new {@link CreatePitResponse} object
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static CreatePitResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    /**
     * Returns the inner from XContent.
     *
     * @param parser the parser
     * @return the inner from XContent
     * @throws IOException if an I/O error occurs
     */
    public static CreatePitResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        int successfulShards = -1;
        int totalShards = -1;
        int skippedShards = 0;
        int failedShards = 0;
        String id = null;
        long creationTime = 0;
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CREATION_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    creationTime = parser.longValue();
                } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    id = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BroadcastShardsHeader._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (BroadcastShardsHeader.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                failedShards = parser.intValue(); // we don't need it but need to consume it
                            } else if (BroadcastShardsHeader.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (BroadcastShardsHeader.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (BroadcastShardsHeader.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (BroadcastShardsHeader.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }

        return new CreatePitResponse(
            id,
            creationTime,
            totalShards,
            successfulShards,
            skippedShards,
            failedShards,
            failures.toArray(ShardSearchFailure.EMPTY_ARRAY)
        );
    }

    /**
     * Returns the creation time.
     *
     * @return the creation time
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * The failed number of shards the search was executed on.
     *
     * @return the failed shards
     */
    public int getFailedShards() {
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     *
     * @return the shard failures
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(skippedShards);
        out.writeLong(creationTime);
        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
    }

    /**
     * Returns the identifier.
     *
     * @return the identifier
     */
    public String getId() {
        return id;
    }

    /**
     * The total number of shards the create pit operation was executed on.
     *
     * @return the total shards
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the create pit operation was executed on.
     *
     * @return the successful shards
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * Returns the skipped shards.
     *
     * @return the skipped shards
     */
    public int getSkippedShards() {
        return skippedShards;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }
}
