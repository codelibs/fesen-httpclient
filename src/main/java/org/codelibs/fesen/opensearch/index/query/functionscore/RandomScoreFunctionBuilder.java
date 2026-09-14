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

package org.codelibs.fesen.opensearch.index.query.functionscore;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a random score for the matched documents
 *
 * @opensearch.internal
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder<RandomScoreFunctionBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RandomScoreFunctionBuilder.class);

    /**
     * The NAME constant.
     */
    public static final String NAME = "random_score";
    private String field;
    private Integer seed;

    /**
     * Creates a new RandomScoreFunctionBuilder.
     */
    public RandomScoreFunctionBuilder() {}

    /**
     * Creates a new RandomScoreFunctionBuilder.
     *
     * @param functionName the function name
     */
    public RandomScoreFunctionBuilder(@Nullable String functionName) {
        setFunctionName(functionName);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (seed != null) {
            out.writeBoolean(true);
            out.writeInt(seed);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(field);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Sets the seed based on which the random number will be generated. Using the same seed is guaranteed to generate the same
     * random number for a specific doc.
     *
     * @param seed The seed.
     * @return the seed
     */
    public RandomScoreFunctionBuilder seed(int seed) {
        this.seed = seed;
        return this;
    }

    /**
     * seed variant taking a long value.
     * @param seed the seed
     * @return the seed
     * @see #seed(int)
     */
    public RandomScoreFunctionBuilder seed(long seed) {
        this.seed = hash(seed);
        return this;
    }

    /**
     * seed variant taking a String value.
     * @param seed the seed
     * @return the seed
     * @see #seed(int)
     */
    public RandomScoreFunctionBuilder seed(String seed) {
        if (seed == null) {
            throw new IllegalArgumentException("random_score function: seed must not be null");
        }
        this.seed = seed.hashCode();
        return this;
    }

    /**
     * Returns the seed.
     *
     * @return the seed
     */
    public Integer getSeed() {
        return seed;
    }

    /**
     * Set the field to be used for random number generation. This parameter is compulsory
     * when a {@link #seed(int) seed} is set and ignored otherwise. Note that documents that
     * have the same value for a field will get the same score.
     *
     * @param field the field
     * @return this instance
     */
    public RandomScoreFunctionBuilder setField(String field) {
        this.field = field;
        return this;
    }

    /**
     * Get the field to use for random number generation.
     * @return the field
     * @see #setField(String)
     */
    public String getField() {
        return field;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (seed != null) {
            builder.field("seed", seed);
        }
        if (field != null) {
            builder.field("field", field);
        }
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RandomScoreFunctionBuilder functionBuilder) {
        return Objects.equals(this.seed, functionBuilder.seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.seed);
    }

    private static int hash(long value) {
        return Long.hashCode(value);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static RandomScoreFunctionBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("seed".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (parser.numberType() == XContentParser.NumberType.INT) {
                            randomScoreFunctionBuilder.seed(parser.intValue());
                        } else if (parser.numberType() == XContentParser.NumberType.LONG) {
                            randomScoreFunctionBuilder.seed(parser.longValue());
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "random_score seed must be an int, long or string, not '" + token.toString() + "'"
                            );
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        randomScoreFunctionBuilder.seed(parser.text());
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "random_score seed must be an int/long or string, not '" + token.toString() + "'"
                        );
                    }
                } else if ("field".equals(currentFieldName)) {
                    randomScoreFunctionBuilder.setField(parser.text());
                } else if (FunctionScoreQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    randomScoreFunctionBuilder.setFunctionName(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            }
        }
        return randomScoreFunctionBuilder;
    }

}
