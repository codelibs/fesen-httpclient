/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.startree;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

/**
 * Collects one or more @{@link StarTreeNode}'s
 */
@ExperimentalApi
public interface StarTreeNodeCollector {
    /**
     * Called to collect a @{@link StarTreeNode}
     * @param node : Node to collect
     */
    void collectStarTreeNode(StarTreeNode node);

}
