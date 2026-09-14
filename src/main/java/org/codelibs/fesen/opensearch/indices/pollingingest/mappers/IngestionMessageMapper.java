/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.indices.pollingingest.mappers;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.util.Locale;

/**
 * Namespace for the ingestion-mapper setting a client reads back from index metadata.
 *
 * <p>Mapping an ingested message is a node-side concern and is not carried over; only the mapper
 * type names that appear in index settings survive here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.3.0")
public interface IngestionMessageMapper {

    /**
     * The mapper an index uses to turn an ingested message into a document.
     *
     * @opensearch.api
     */
    enum MapperType {
        /**
         * The DEFAULT value.
         */
        DEFAULT("default"),
        /**
         * The RAW_PAYLOAD value.
         */
        RAW_PAYLOAD("raw_payload"),
        /**
         * The FIELD_MAPPING value.
         */
        FIELD_MAPPING("field_mapping");

        private final String name;

        MapperType(String name) {
            this.name = name;
        }

        /**
         * Returns the name.
         *
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * Creates an instance from string.
         *
         * @param value the value
         * @return the new string
         */
        public static MapperType fromString(String value) {
            for (MapperType type : MapperType.values()) {
                if (type.name.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unknown ingestion mapper type: %s. Valid values are: default, raw_payload, field_mapping",
                    value
                )
            );
        }
    }
}
