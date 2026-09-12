/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.mapper;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.index.IndexSettings;
import org.codelibs.fesen.opensearch.index.query.QueryShardContext;
import org.codelibs.fesen.opensearch.search.SearchService;

import java.util.List;
import java.util.Map;

import static org.codelibs.fesen.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Used to create DerivedFieldResolver. It chooses between {@link DefaultDerivedFieldResolver} and {@link NoOpDerivedFieldResolver}
 * depending on whether derived field is enabled.
 */
public class DerivedFieldResolverFactory {

    public static DerivedFieldResolver createResolver(
        QueryShardContext queryShardContext,
        Map<String, Object> derivedFieldsObject,
        List<DerivedField> derivedFields,
        boolean derivedFieldAllowed
    ) {
        boolean derivedFieldsPresent = derivedFieldsPresent(derivedFieldsObject, derivedFields);
        if (derivedFieldsPresent && !derivedFieldAllowed) {
            throw new OpenSearchException(
                "[derived field] queries cannot be executed when '"
                    + IndexSettings.ALLOW_DERIVED_FIELDS.getKey()
                    + "' or '"
                    + SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING.getKey()
                    + "' is set to false."
            );
        }
        if (derivedFieldsPresent && queryShardContext.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[derived field] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        if (derivedFieldAllowed) {
            return new DefaultDerivedFieldResolver(queryShardContext, derivedFieldsObject, derivedFields);
        } else {
            return new NoOpDerivedFieldResolver();
        }
    }

    private static boolean derivedFieldsPresent(Map<String, Object> derivedFieldsObject, List<DerivedField> derivedFields) {
        return (derivedFieldsObject != null && !derivedFieldsObject.isEmpty()) || (derivedFields != null && !derivedFields.isEmpty());
    }
}
