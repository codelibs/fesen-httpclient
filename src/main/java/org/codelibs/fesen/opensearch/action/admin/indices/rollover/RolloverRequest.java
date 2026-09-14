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

package org.codelibs.fesen.opensearch.action.admin.indices.rollover;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias or increment data stream generation upon satisfying conditions
 * <p>
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should also go to that client class.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private static final ObjectParser<RolloverRequest, Void> PARSER = new ObjectParser<>("rollover");
    private static final ObjectParser<Map<String, Condition<?>>, Void> CONDITION_PARSER = new ObjectParser<>("conditions");

    private static final ParseField CONDITIONS = new ParseField("conditions");
    private static final ParseField MAX_AGE_CONDITION = new ParseField(MaxAgeCondition.NAME);
    private static final ParseField MAX_DOCS_CONDITION = new ParseField(MaxDocsCondition.NAME);
    private static final ParseField MAX_SIZE_CONDITION = new ParseField(MaxSizeCondition.NAME);

    static {
        CONDITION_PARSER.declareString(
            (conditions, s) -> conditions.put(MaxAgeCondition.NAME, new MaxAgeCondition(TimeValue.parseTimeValue(s, MaxAgeCondition.NAME))),
            MAX_AGE_CONDITION
        );
        CONDITION_PARSER.declareLong(
            (conditions, value) -> conditions.put(MaxDocsCondition.NAME, new MaxDocsCondition(value)),
            MAX_DOCS_CONDITION
        );
        CONDITION_PARSER.declareString(
            (conditions, s) -> conditions.put(
                MaxSizeCondition.NAME,
                new MaxSizeCondition(ByteSizeValue.parseBytesSizeValue(s, MaxSizeCondition.NAME))
            ),
            MAX_SIZE_CONDITION
        );

        PARSER.declareField(
            (parser, request, context) -> CONDITION_PARSER.parse(parser, request.conditions, null),
            CONDITIONS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            (parser, request, context) -> request.createIndexRequest.settings(parser.map()),
            CreateIndexRequest.SETTINGS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField((parser, request, context) -> {
            // a type is not included, add a dummy _doc type
            Map<String, Object> mappings = parser.map();
            if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
                throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
            }
            request.createIndexRequest.mapping(mappings);
        }, CreateIndexRequest.MAPPINGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            (parser, request, context) -> request.createIndexRequest.aliases(parser.map()),
            CreateIndexRequest.ALIASES,
            ObjectParser.ValueType.OBJECT
        );
    }

    private String rolloverTarget;
    private String newIndexName;
    private boolean dryRun;
    private final Map<String, Condition<?>> conditions = new HashMap<>(2);
    // the index name "_na_" is never read back, what matters are settings, mappings and aliases
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

    RolloverRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = createIndexRequest.validate();
        if (rolloverTarget == null) {
            validationException = addValidationError("rollover target is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rolloverTarget);
        out.writeOptionalString(newIndexName);
        out.writeBoolean(dryRun);
        out.writeVInt(conditions.size());
        for (Condition<?> condition : conditions.values()) {
            if (condition.includedInVersion(out.getVersion())) {
                out.writeNamedWriteable(condition);
            }
        }
        createIndexRequest.writeTo(out);
    }

    @Override
    public String[] indices() {
        return new String[] { rolloverTarget };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Sets the rollover target to rollover to another index
     *
     * @param rolloverTarget the rollover target
     */
    public void setRolloverTarget(String rolloverTarget) {
        this.rolloverTarget = rolloverTarget;
    }

    /**
     * Sets the alias to rollover to another index
     *
     * @param newIndexName the new index name
     */
    public void setNewIndexName(String newIndexName) {
        this.newIndexName = newIndexName;
    }

    /**
     * Returns the dry run flag.
     *
     * @return the dry run flag
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Returns the conditions.
     *
     * @return the conditions
     */
    public Map<String, Condition<?>> getConditions() {
        return conditions;
    }

    /**
     * Returns the rollover target.
     *
     * @return the rollover target
     */
    public String getRolloverTarget() {
        return rolloverTarget;
    }

    /**
     * Returns the new index name.
     *
     * @return the new index name
     */
    public String getNewIndexName() {
        return newIndexName;
    }

    /**
     * Returns the inner {@link CreateIndexRequest}. Allows to configure mappings, settings and aliases for the new index.
     *
     * @return the create index request
     */
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }
}
