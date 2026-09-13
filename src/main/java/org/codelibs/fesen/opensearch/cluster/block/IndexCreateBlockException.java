/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.block;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Set;

/**
 * Internal exception on obtaining an index create block
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexCreateBlockException extends ClusterBlockException {

    public IndexCreateBlockException(Set<ClusterBlock> globalLevelBlocks) {
        super(globalLevelBlocks);
    }
}
