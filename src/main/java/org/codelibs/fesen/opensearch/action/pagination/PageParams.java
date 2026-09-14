/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.pagination;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 *
 * Class specific to paginated queries, which will contain common query params required by a paginated API.
 */
@PublicApi(since = "2.18.0")
public class PageParams implements Writeable {

    /**
     * The PARAM_ASC_SORT_VALUE constant.
     */
    public static final String PARAM_ASC_SORT_VALUE = "asc";

    private final String requestedTokenStr;
    private final String sort;
    private final int size;

    /**
     * Creates a new PageParams.
     *
     * @param requestedToken the requested token
     * @param sort the sort
     * @param size the size
     */
    public PageParams(String requestedToken, String sort, int size) {
        this.requestedTokenStr = requestedToken;
        this.sort = sort;
        this.size = size;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(requestedTokenStr);
        out.writeOptionalString(sort);
        out.writeInt(size);
    }

    // Overriding equals and hashcode for tests
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageParams that = (PageParams) o;
        return this.size == that.size
            && Objects.equals(this.requestedTokenStr, that.requestedTokenStr)
            && Objects.equals(this.sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestedTokenStr, sort, size);
    }
}
