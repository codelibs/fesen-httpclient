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

package org.codelibs.fesen.opensearch.action.support;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesOptions implements ToXContentFragment {

    /**
     * The wildcard states.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum WildcardStates {
        /**
         * The OPEN value.
         */
        OPEN,
        /**
         * The CLOSED value.
         */
        CLOSED,
        /**
         * The HIDDEN value.
         */
        HIDDEN;

        /**
         * The NONE constant.
         */
        public static final EnumSet<WildcardStates> NONE = EnumSet.noneOf(WildcardStates.class);

        /**
         * Writes this instance to the given content builder.
         *
         * @param states the states
         * @param builder the content builder
         * @return the XContent
         * @throws IOException if an I/O error occurs
         */
        public static XContentBuilder toXContent(EnumSet<WildcardStates> states, XContentBuilder builder) throws IOException {
            if (states.isEmpty()) {
                builder.field("expand_wildcards", "none");
            } else if (states.containsAll(EnumSet.allOf(WildcardStates.class))) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field(
                    "expand_wildcards",
                    states.stream().map(state -> state.toString().toLowerCase(Locale.ROOT)).collect(Collectors.joining(","))
                );
            }
            return builder;
        }
    }

    /**
     * The options.
     *
     * @opensearch.internal
     */
    public enum Option {
        /**
         * The IGNORE_UNAVAILABLE value.
         */
        IGNORE_UNAVAILABLE,
        /**
         * The IGNORE_ALIASES value.
         */
        IGNORE_ALIASES,
        /**
         * The ALLOW_NO_INDICES value.
         */
        ALLOW_NO_INDICES,
        /**
         * The FORBID_ALIASES_TO_MULTIPLE_INDICES value.
         */
        FORBID_ALIASES_TO_MULTIPLE_INDICES,
        /**
         * The FORBID_CLOSED_INDICES value.
         */
        FORBID_CLOSED_INDICES,
        /**
         * The IGNORE_THROTTLED value.
         */
        IGNORE_THROTTLED;

        /**
         * The NONE constant.
         */
        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    /**
     * The STRICT_EXPAND_OPEN constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    /**
     * The STRICT_EXPAND_OPEN_HIDDEN constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    /**
     * The LENIENT_EXPAND_OPEN constant.
     */
    public static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN)
    );
    /**
     * The LENIENT_EXPAND_OPEN_HIDDEN constant.
     */
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    /**
     * The LENIENT_EXPAND_OPEN_CLOSED constant.
     */
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    /**
     * The LENIENT_EXPAND_OPEN_CLOSED_HIDDEN constant.
     */
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    /**
     * The STRICT_EXPAND_OPEN_CLOSED constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    /**
     * The STRICT_EXPAND_OPEN_CLOSED_HIDDEN constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    /**
     * The STRICT_EXPAND_OPEN_FORBID_CLOSED constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    /**
     * The STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    /**
     * The STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED constant.
     */
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    /**
     * The STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED constant.
     */
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.noneOf(WildcardStates.class)
    );

    private final EnumSet<Option> options;
    private final EnumSet<WildcardStates> expandWildcards;

    /**
     * Creates a new IndicesOptions.
     *
     * @param options the options
     * @param expandWildcards the expand wildcards
     */
    public IndicesOptions(EnumSet<Option> options, EnumSet<WildcardStates> expandWildcards) {
        this.options = options;
        this.expandWildcards = expandWildcards;
    }

    /**
     * Returns the ignore unavailable.
     *
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return options.contains(Option.IGNORE_UNAVAILABLE);
    }

    /**
     * Returns the allow no indices.
     *
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     *         The `_all` string or empty list of indices count as wildcard expressions too.
     *         Also when an alias points to a closed index this option decides if no concrete indices
     *         are allowed.
     */
    public boolean allowNoIndices() {
        return options.contains(Option.ALLOW_NO_INDICES);
    }

    /**
     * Expands the wildcards open.
     *
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return expandWildcards.contains(WildcardStates.OPEN);
    }

    /**
     * Expands the wildcards closed.
     *
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return expandWildcards.contains(WildcardStates.CLOSED);
    }

    /**
     * Expands the wildcards hidden.
     *
     * @return Whether wildcard expressions should get expanded to hidden indices
     */
    public boolean expandWildcardsHidden() {
        return expandWildcards.contains(WildcardStates.HIDDEN);
    }

    /**
     * Returns the forbid closed indices.
     *
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return options.contains(Option.FORBID_CLOSED_INDICES);
    }

    /**
     * Returns the allow aliases to multiple indices.
     *
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        // true is default here, for bw comp we keep the first 16 values
        // in the array same as before + the default value for the new flag
        return options.contains(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES) == false;
    }

    /**
     * Returns the ignore aliases.
     *
     * @return whether aliases should be ignored (when resolving a wildcard)
     */
    public boolean ignoreAliases() {
        return options.contains(Option.IGNORE_ALIASES);
    }

    /**
     * Returns the ignore throttled.
     *
     * @return whether indices that are marked as throttled should be ignored
     */
    public boolean ignoreThrottled() {
        return options.contains(Option.IGNORE_THROTTLED);
    }

    /**
     * Returns the expand wildcards.
     *
     * @return a copy of the {@link WildcardStates} that these indices options will expand to
     */
    public EnumSet<WildcardStates> getExpandWildcards() {
        return EnumSet.copyOf(expandWildcards);
    }

    /**
     * Writes the indices options.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeIndicesOptions(StreamOutput out) throws IOException {
        EnumSet<Option> options = this.options;
        out.writeEnumSet(options);
        out.writeEnumSet(expandWildcards);
    }

    /**
     * Reads the indices options.
     *
     * @param in the input to read from
     * @return the indices options
     * @throws IOException if an I/O error occurs
     */
    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        EnumSet<Option> options = in.readEnumSet(Option.class);
        EnumSet<WildcardStates> states = in.readEnumSet(WildcardStates.class);
        return new IndicesOptions(options, states);
    }

    /**
     * Creates an instance from options.
     *
     * @param ignoreUnavailable the ignore unavailable
     * @param allowNoIndices the allow no indices
     * @param expandToOpenIndices the expand to open indices
     * @param expandToClosedIndices the expand to closed indices
     * @return the new options
     */
    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices
    ) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, false);
    }

    /**
     * Creates an instance from options.
     *
     * @param ignoreUnavailable the ignore unavailable
     * @param allowNoIndices the allow no indices
     * @param expandToOpenIndices the expand to open indices
     * @param expandToClosedIndices the expand to closed indices
     * @param expandToHiddenIndices the expand to hidden indices
     * @return the new options
     */
    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            expandToHiddenIndices,
            true,
            false,
            false,
            false
        );
    }

    /**
     * Creates an instance from options.
     *
     * @param ignoreUnavailable the ignore unavailable
     * @param allowNoIndices the allow no indices
     * @param expandToOpenIndices the expand to open indices
     * @param expandToClosedIndices the expand to closed indices
     * @param allowAliasesToMultipleIndices the allow aliases to multiple indices
     * @param forbidClosedIndices the forbid closed indices
     * @param ignoreAliases the ignore aliases
     * @param ignoreThrottled the ignore throttled
     * @return the new options
     */
    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            false,
            allowAliasesToMultipleIndices,
            forbidClosedIndices,
            ignoreAliases,
            ignoreThrottled
        );
    }

    /**
     * Creates an instance from options.
     *
     * @param ignoreUnavailable the ignore unavailable
     * @param allowNoIndices the allow no indices
     * @param expandToOpenIndices the expand to open indices
     * @param expandToClosedIndices the expand to closed indices
     * @param expandToHiddenIndices the expand to hidden indices
     * @param allowAliasesToMultipleIndices the allow aliases to multiple indices
     * @param forbidClosedIndices the forbid closed indices
     * @param ignoreAliases the ignore aliases
     * @param ignoreThrottled the ignore throttled
     * @return the new options
     */
    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        final EnumSet<Option> opts = EnumSet.noneOf(Option.class);
        final EnumSet<WildcardStates> wildcards = EnumSet.noneOf(WildcardStates.class);

        if (ignoreUnavailable) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        if (allowNoIndices) {
            opts.add(Option.ALLOW_NO_INDICES);
        }
        if (expandToOpenIndices) {
            wildcards.add(WildcardStates.OPEN);
        }
        if (expandToClosedIndices) {
            wildcards.add(WildcardStates.CLOSED);
        }
        if (expandToHiddenIndices) {
            wildcards.add(WildcardStates.HIDDEN);
        }
        if (allowAliasesToMultipleIndices == false) {
            opts.add(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (forbidClosedIndices) {
            opts.add(Option.FORBID_CLOSED_INDICES);
        }
        if (ignoreAliases) {
            opts.add(Option.IGNORE_ALIASES);
        }
        if (ignoreThrottled) {
            opts.add(Option.IGNORE_THROTTLED);
        }
        return new IndicesOptions(opts, wildcards);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("expand_wildcards");
        for (WildcardStates expandWildcard : expandWildcards) {
            builder.value(expandWildcard.toString().toLowerCase(Locale.ROOT));
        }
        builder.endArray();
        builder.field("ignore_unavailable", ignoreUnavailable());
        builder.field("allow_no_indices", allowNoIndices());
        builder.field("ignore_throttled", ignoreThrottled());
        return builder;
    }

    private static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField("expand_wildcards");
    private static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField("ignore_unavailable");
    private static final ParseField IGNORE_THROTTLED_FIELD = new ParseField("ignore_throttled");
    private static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField("allow_no_indices");

    /**
     * Returns the strict expand open.
     *
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN;
    }

    /**
     * Returns the strict expand open hidden.
     *
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices, includes hidden
     *         indices, and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpenHidden() {
        return STRICT_EXPAND_OPEN_HIDDEN;
    }

    /**
     * Returns the strict expand open and forbid closed.
     *
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     *         use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED;
    }

    /**
     * Returns the strict expand open and forbid closed ignore throttled.
     *
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error),
     *         forbids the use of closed indices by throwing an error and ignores indices that are throttled.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosedIgnoreThrottled() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED;
    }

    /**
     * Returns the strict expand.
     *
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED;
    }

    /**
     * Returns the strict expand hidden.
     *
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed indices, includes
     *         hidden indices, and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandHidden() {
        return STRICT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    /**
     * Returns the strict single index no expand forbid closed.
     *
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * Returns the lenient expand open.
     *
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN;
    }

    /**
     * Returns the lenient expand hidden.
     *
     * @return indices options that ignores unavailable indices,  expands wildcards to all open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandHidden() {
        return LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        IndicesOptions other = (IndicesOptions) obj;
        return options.equals(other.options) && expandWildcards.equals(other.expandWildcards);
    }

    @Override
    public int hashCode() {
        int result = options.hashCode();
        return 31 * result + expandWildcards.hashCode();
    }

    @Override
    public String toString() {
        return "IndicesOptions["
            + "ignore_unavailable="
            + ignoreUnavailable()
            + ", allow_no_indices="
            + allowNoIndices()
            + ", expand_wildcards_open="
            + expandWildcardsOpen()
            + ", expand_wildcards_closed="
            + expandWildcardsClosed()
            + ", expand_wildcards_hidden="
            + expandWildcardsHidden()
            + ", allow_aliases_to_multiple_indices="
            + allowAliasesToMultipleIndices()
            + ", forbid_closed_indices="
            + forbidClosedIndices()
            + ", ignore_aliases="
            + ignoreAliases()
            + ", ignore_throttled="
            + ignoreThrottled()
            + ']';
    }
}
