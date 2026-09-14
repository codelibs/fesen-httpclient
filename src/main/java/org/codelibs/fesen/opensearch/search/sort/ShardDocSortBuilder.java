/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.sort;

import org.apache.lucene.search.SortField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.query.QueryRewriteContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Sort builder for the pseudo‐field "_shard_doc", which tiebreaks by {@code (shardId << 32) | globalDocId}.
 */
public class ShardDocSortBuilder extends SortBuilder<ShardDocSortBuilder> {

    /**
     * The NAME constant.
     */
    public static final String NAME = "_shard_doc";

    // parser for JSON: { "_shard_doc": { "order":"asc" } }
    private static final ObjectParser<ShardDocSortBuilder, Void> PARSER = new ObjectParser<>(NAME, ShardDocSortBuilder::new);

    static {
        PARSER.declareString((b, s) -> b.order(SortOrder.fromString(s)), ORDER_FIELD);
    }

    /**
     * Creates a new ShardDocSortBuilder.
     */
    public ShardDocSortBuilder() {
        this.order = SortOrder.ASC; // default to ASC
    }

    /**
     * Creates a new ShardDocSortBuilder.
     *
     * @param other the other instance
     */
    public ShardDocSortBuilder(ShardDocSortBuilder other) {
        this.order = other.order;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        order.writeTo(out);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @param fieldName the field name
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static ShardDocSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }

        switch (token) {
            case START_OBJECT:
                return PARSER.parse(parser, null); // { "_shard_doc": { "order": "asc" } }

            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_BOOLEAN:
            case VALUE_NULL:
                return new ShardDocSortBuilder(); // Scalar shorthand: "_shard_doc" → defaults to ASC

            case START_ARRAY:
                throw new org.codelibs.fesen.opensearch.core.xcontent.XContentParseException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] Expected START_OBJECT or scalar but was: START_ARRAY"
                );

            default:
                throw new org.codelibs.fesen.opensearch.core.xcontent.XContentParseException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] Expected START_OBJECT or scalar but was: " + token
                );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public ShardDocSortBuilder rewrite(QueryRewriteContext ctx) {
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ShardDocSortBuilder other = (ShardDocSortBuilder) obj;
        return order == other.order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(order);
    }
}
