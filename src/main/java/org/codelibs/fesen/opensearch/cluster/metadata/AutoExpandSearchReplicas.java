/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.common.Booleans;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;


/**
 * This class acts as a functional wrapper around the {@code index.auto_expand_search_replicas} setting.
 * This setting's value expands into a minimum and maximum value, requiring special handling based on the
 * number of search nodes in the cluster. This class handles parsing and simplifies access to these values.
 *
 * @opensearch.internal
 */
public final class AutoExpandSearchReplicas {
    // the value we recognize in the "max" position to mean all the search nodes
    private static final String ALL_NODES_VALUE = "all";

    private static final AutoExpandSearchReplicas FALSE_INSTANCE = new AutoExpandSearchReplicas(0, 0, false);

    /**
     * The SETTING constant.
     */
    public static final Setting<AutoExpandSearchReplicas> SETTING = new Setting<>(
        IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS,
        "false",
        AutoExpandSearchReplicas::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    private static AutoExpandSearchReplicas parse(String value) {
        final int min;
        final int max;
        if (Booleans.isFalse(value)) {
            return FALSE_INSTANCE;
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS + "] from value: [" + value + "] at index " + dash
            );
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS + "] from value: [" + value + "] at index " + dash,
                e
            );
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "failed to parse ["
                        + IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS
                        + "] from value: ["
                        + value
                        + "] at index "
                        + dash,
                    e
                );
            }
        }
        return new AutoExpandSearchReplicas(min, max, true);
    }

    private final int minSearchReplicas;
    private final int maxSearchReplicas;
    private final boolean enabled;

    private AutoExpandSearchReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException(
                "["
                    + IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS
                    + "] minSearchReplicas must be =< maxSearchReplicas but wasn't "
                    + minReplicas
                    + " > "
                    + maxReplicas
            );
        }
        this.minSearchReplicas = minReplicas;
        this.maxSearchReplicas = maxReplicas;
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return enabled ? minSearchReplicas + "-" + maxSearchReplicas : "false";
    }

}
