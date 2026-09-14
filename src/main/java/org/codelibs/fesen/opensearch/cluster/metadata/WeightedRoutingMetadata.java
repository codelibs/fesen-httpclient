/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.AbstractNamedDiffable;
import org.codelibs.fesen.opensearch.cluster.NamedDiff;
import org.codelibs.fesen.opensearch.cluster.routing.WeightedRouting;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Contains metadata for weighted routing
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class WeightedRoutingMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    /**
     * The TYPE constant.
     */
    public static final String TYPE = "weighted_shard_routing";
    /**
     * The AWARENESS constant.
     */
    public static final String AWARENESS = "awareness";
    /**
     * The VERSION constant.
     */
    public static final String VERSION = "_version";
    /**
     * The INITIAL_VERSION constant.
     */
    public static final long INITIAL_VERSION = -1;
    /**
     * The VERSION_UNSET_VALUE constant.
     */
    public static final long VERSION_UNSET_VALUE = -2;
    /**
     * The WEIGHED_AWAY_WEIGHT constant.
     */
    public static final int WEIGHED_AWAY_WEIGHT = 0;

    /**
     * Returns the version.
     *
     * @return the version
     */
    public long getVersion() {
        return version;
    }

    private long version;
    private WeightedRouting weightedRouting;

    /**
     * Returns the weighted routing.
     *
     * @return the weighted routing
     */
    public WeightedRouting getWeightedRouting() {
        return weightedRouting;
    }

    /**
     * Sets the weighted routing.
     *
     * @param weightedRouting the weighted routing
     * @return this instance
     */
    public WeightedRoutingMetadata setWeightedRouting(WeightedRouting weightedRouting) {
        this.weightedRouting = weightedRouting;
        return this;
    }

    /**
     * Creates a new WeightedRoutingMetadata.
     *
     * @param weightedRouting the weighted routing
     * @param version the version
     */
    public WeightedRoutingMetadata(WeightedRouting weightedRouting, long version) {
        this.weightedRouting = weightedRouting;
        this.version = version;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_2_4_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (weightedRouting != null) {
            weightedRouting.writeTo(out);
            out.writeLong(version);
        }
    }

    /**
     * Reads the diff from.
     *
     * @param in the input to read from
     * @return the diff from
     * @throws IOException if an I/O error occurs
     */
    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static WeightedRoutingMetadata fromXContent(XContentParser parser) throws IOException {
        String attrKey = null;
        Double attrValue;
        String attributeName = "";
        Map<String, Double> weights = new HashMap<>();
        WeightedRouting weightedRouting;
        XContentParser.Token token;
        String awarenessField;
        String versionAttr = null;
        long version = VERSION_UNSET_VALUE;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String attr = parser.currentName();
                if (attr != null && attr.equals(VERSION)) {
                    versionAttr = parser.currentName();
                    continue;
                } else {
                    awarenessField = parser.currentName();
                }
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse weighted routing metadata  [{}], expected object", awarenessField);
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    attributeName = parser.currentName();
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException(
                            "failed to parse weighted routing metadata  [{}], expected object",
                            attributeName
                        );
                    }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            attrKey = parser.currentName();

                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (attrKey != null && attrKey.equals(VERSION)) {
                                version = Long.parseLong(parser.text());
                            } else {
                                attrValue = Double.parseDouble(parser.text());
                                weights.put(attrKey, attrValue);
                            }

                        } else {
                            throw new OpenSearchParseException(
                                "failed to parse weighted routing metadata attribute " + "[{}], unknown type",
                                attributeName
                            );
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (versionAttr != null && versionAttr.equals(VERSION)) {
                    version = Long.parseLong(parser.text());
                }
            }
        }
        weightedRouting = new WeightedRouting(attributeName, weights);
        return new WeightedRoutingMetadata(weightedRouting, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedRoutingMetadata that = (WeightedRoutingMetadata) o;
        return weightedRouting.equals(that.weightedRouting) && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(weightedRouting.hashCode(), version);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContent(weightedRouting, builder, version);
        return builder;
    }

    /**
     * Writes this instance to the given content builder.
     *
     * @param weightedRouting the weighted routing
     * @param builder the content builder
     * @param version the version
     * @throws IOException if an I/O error occurs
     */
    public static void toXContent(WeightedRouting weightedRouting, XContentBuilder builder, long version) throws IOException {
        builder.startObject(AWARENESS);
        if (weightedRouting.isSet()) {
            builder.startObject(weightedRouting.attributeName());
            for (Map.Entry<String, Double> entry : weightedRouting.weights().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        builder.field(VERSION, version);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
