/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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
package org.codelibs.fesen.client.action.indices.create;

import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.Settings.Builder;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;

public class HttpCreateIndexRequest extends CreateIndexRequest {

    private final CreateIndexRequest request;

    public HttpCreateIndexRequest(final CreateIndexRequest request) {
        this.request = request;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @Override
    public CreateIndexRequest source(final BytesReference source, final XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        sourceAsMap = prepareMappings(sourceAsMap);
        source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    // RestCreateIndexAction#prepareMappings
    public static Map<String, Object> prepareMappings(final Map<String, Object> source) {
        if (!source.containsKey("mappings") || !(source.get("mappings") instanceof Map)) {
            return source;
        }

        final Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        final Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }

    @Override
    public void setParentTask(final String parentTaskNode, final long parentTaskId) {
        request.setParentTask(parentTaskNode, parentTaskId);
    }

    @Override
    public void remoteAddress(final TransportAddress remoteAddress) {
        request.remoteAddress(remoteAddress);
    }

    @Override
    public TransportAddress remoteAddress() {
        return request.remoteAddress();
    }

    @Override
    public Task createTask(final long id, final String type, final String action, final TaskId parentTaskId,
            final Map<String, String> headers) {
        return request.createTask(id, type, action, parentTaskId, headers);
    }

    @Override
    public boolean getShouldStoreResult() {
        return request.getShouldStoreResult();
    }

    @Override
    public boolean includeDataStreams() {
        return request.includeDataStreams();
    }

    @Override
    public void setParentTask(final TaskId taskId) {
        request.setParentTask(taskId);
    }

    @Override
    public String getDescription() {
        return request.getDescription();
    }

    @Override
    public TaskId getParentTask() {
        return request.getParentTask();
    }

    @Override
    public int hashCode() {
        return request.hashCode();
    }

    @Override
    public TimeValue ackTimeout() {
        return request.ackTimeout();
    }

    @Override
    public boolean equals(final Object obj) {
        return request.equals(obj);
    }

    @Override
    public ActionRequestValidationException validate() {
        return request.validate();
    }

    @Override
    public String[] indices() {
        return request.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return request.indicesOptions();
    }

    @Override
    public String index() {
        return request.index();
    }

    @Override
    public CreateIndexRequest index(final String index) {
        request.index(index);
        return this;
    }

    @Override
    public Settings settings() {
        return request.settings();
    }

    @Override
    public String cause() {
        return request.cause();
    }

    @Override
    public CreateIndexRequest settings(final Builder settings) {
        request.settings(settings);
        return this;
    }

    @Override
    public CreateIndexRequest settings(final Settings settings) {
        request.settings(settings);
        return this;
    }

    @Override
    public CreateIndexRequest settings(final String source, final XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    @Override
    public CreateIndexRequest settings(final Map<String, ?> source) {
        request.settings(source);
        return this;
    }

    @Override
    public CreateIndexRequest mapping(final String mapping) {
        request.mapping(mapping);
        return this;
    }

    @Override
    public String toString() {
        return request.toString();
    }

    @Override
    public CreateIndexRequest cause(final String cause) {
        request.cause(cause);
        return this;
    }

    @Override
    public CreateIndexRequest aliases(final Map<String, ?> source) {
        request.aliases(source);
        return this;
    }

    @Override
    public CreateIndexRequest aliases(final BytesReference source) {
        request.aliases(source);
        return this;
    }

    @Override
    public CreateIndexRequest alias(final Alias alias) {
        request.alias(alias);
        return this;
    }

    @Override
    public CreateIndexRequest source(final Map<String, ?> source, final DeprecationHandler deprecationHandler) {
        request.source(source, deprecationHandler);
        return this;
    }

    @Override
    public String mappings() {
        return request.mappings();
    }

    @Override
    public Set<Alias> aliases() {
        return request.aliases();
    }

    @Override
    public ActiveShardCount waitForActiveShards() {
        return request.waitForActiveShards();
    }

    @Override
    public CreateIndexRequest waitForActiveShards(final ActiveShardCount waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    @Override
    public CreateIndexRequest waitForActiveShards(final int waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        request.writeTo(out);
    }
}
