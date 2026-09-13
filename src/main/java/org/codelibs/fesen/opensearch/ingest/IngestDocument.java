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

package org.codelibs.fesen.opensearch.ingest;

import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.script.TemplateScript;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Represents a single document being captured before indexing and holds the source and metadata (like id, type and index).
 *
 * @opensearch.internal
 */
public final class IngestDocument {

    public static final String INGEST_KEY = "_ingest";
    public static final String PIPELINE_CYCLE_ERROR_MESSAGE = "Cycle detected for pipeline: ";
    private static final String INGEST_KEY_PREFIX = INGEST_KEY + ".";
    private static final String SOURCE_PREFIX = "_source" + ".";

    static final String TIMESTAMP = "timestamp";

    private final Map<String, Object> sourceAndMetadata;
    private final Map<String, Object> ingestMetadata;

    // Contains all pipelines that have been executed for this document
    private final Set<String> executedPipelines = new LinkedHashSet<>();

    /**
     * Copy constructor that creates a new {@link IngestDocument} which has exactly the same properties as the one provided as argument
     */
    public IngestDocument(IngestDocument other) {
        this(deepCopyMap(other.sourceAndMetadata), deepCopyMap(other.ingestMetadata));
    }

    /**
     * Constructor needed for testing that allows to create a new {@link IngestDocument} given the provided opensearch metadata,
     * source and ingest metadata. This is needed because the ingest metadata will be initialized with the current timestamp at
     * init time, which makes equality comparisons impossible in tests.
     */
    public IngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        this.sourceAndMetadata = sourceAndMetadata;
        this.ingestMetadata = ingestMetadata;
    }

    /**
     * Does the same thing as {@link #extractMetadata} but does not mutate the map.
     */
    public Map<Metadata, Object> getMetadata() {
        Map<Metadata, Object> metadataMap = new EnumMap<>(Metadata.class);
        for (Metadata metadata : Metadata.values()) {
            metadataMap.put(metadata, sourceAndMetadata.get(metadata.getFieldName()));
        }
        return metadataMap;
    }

    /**
     * Returns the available ingest metadata fields, by default only timestamp, but it is possible to set additional ones.
     * Use only for reading values, modify them instead using {@link #setFieldValue(String, Object)} and {@link #removeField(String)}
     */
    public Map<String, Object> getIngestMetadata() {
        return this.ingestMetadata;
    }

    /**
     * Returns the document including its metadata fields, unless {@link #extractMetadata()} has been called, in which case the
     * metadata fields will not be present anymore.
     * Modify the document instead using {@link #setFieldValue(String, Object)} and {@link #removeField(String)}
     */
    public Map<String, Object> getSourceAndMetadata() {
        return this.sourceAndMetadata;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> deepCopyMap(Map<K, V> source) {
        CollectionUtils.ensureNoSelfReferences(source, "IngestDocument: Self reference present in object.");
        return (Map<K, V>) deepCopy(source);
    }

    public static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copy = new HashMap<>(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), deepCopy(entry.getValue()));
            }
            return copy;
        } else if (value instanceof List<?> listValue) {
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(deepCopy(itemValue));
            }
            return copy;
        } else if (value instanceof byte[] bytes) {
            return Arrays.copyOf(bytes, bytes.length);
        } else if (value == null
            || value instanceof Byte
            || value instanceof Character
            || value instanceof Short
            || value instanceof String
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Float
            || value instanceof Double
            || value instanceof Boolean
            || value instanceof ZonedDateTime) {
                return value;
            } else if (value instanceof Date date) {
                return date.clone();
            } else {
                throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
            }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IngestDocument other = (IngestDocument) obj;
        return Objects.equals(sourceAndMetadata, other.sourceAndMetadata) && Objects.equals(ingestMetadata, other.ingestMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceAndMetadata, ingestMetadata);
    }

    @Override
    public String toString() {
        return "IngestDocument{" + " sourceAndMetadata=" + sourceAndMetadata + ", ingestMetadata=" + ingestMetadata + '}';
    }

    /**
     * The ingest metadata.
     *
     * @opensearch.internal
     */
    public enum Metadata {
        INDEX("_index"),
        ID("_id"),
        ROUTING("_routing"),
        VERSION("_version"),
        VERSION_TYPE("_version_type"),
        IF_SEQ_NO("_if_seq_no"),
        IF_PRIMARY_TERM("_if_primary_term");

        private final String fieldName;

        Metadata(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    private class FieldPath {

        private final String[] pathElements;
        private final Object initialContext;

        private FieldPath(String path) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            String newPath;
            if (path.startsWith(INGEST_KEY_PREFIX)) {
                initialContext = ingestMetadata;
                newPath = path.substring(INGEST_KEY_PREFIX.length(), path.length());
            } else {
                initialContext = sourceAndMetadata;
                if (path.startsWith(SOURCE_PREFIX)) {
                    newPath = path.substring(SOURCE_PREFIX.length(), path.length());
                } else {
                    newPath = path;
                }
            }
            this.pathElements = newPath.split("\\.");
            if (pathElements.length == 1 && pathElements[0].isEmpty()) {
                throw new IllegalArgumentException("path [" + path + "] is not valid");
            }
        }

    }
}
