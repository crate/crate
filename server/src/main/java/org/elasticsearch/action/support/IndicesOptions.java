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

package org.elasticsearch.action.support;


import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public class IndicesOptions implements ToXContentFragment {

    public enum WildcardStates {
        OPEN,
        CLOSED;

        public static final EnumSet<WildcardStates> NONE = EnumSet.noneOf(WildcardStates.class);
    }

    public enum Option {
        IGNORE_UNAVAILABLE,
        ALLOW_NO_INDICES,
        FORBID_CLOSED_INDICES;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    public static final IndicesOptions STRICT_EXPAND_OPEN =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions LENIENT_EXPAND_OPEN =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
            EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
            EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED));
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES), EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED));
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED =
        new IndicesOptions(EnumSet.of(Option.FORBID_CLOSED_INDICES),
            EnumSet.noneOf(WildcardStates.class));

    private final EnumSet<Option> options;
    private final EnumSet<WildcardStates> expandWildcards;

    public IndicesOptions(EnumSet<Option> options, EnumSet<WildcardStates> expandWildcards) {
        this.options = options;
        this.expandWildcards = expandWildcards;
    }

    private IndicesOptions(Collection<Option> options, Collection<WildcardStates> expandWildcards) {
        this(options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
            expandWildcards.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(expandWildcards));
    }

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return options.contains(Option.IGNORE_UNAVAILABLE);
    }

    /**
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     *         The `_all` string or empty list of indices count as wildcard expressions too.
     *         Also when an alias points to a closed index this option decides if no concrete indices
     *         are allowed.
     */
    public boolean allowNoIndices() {
        return options.contains(Option.ALLOW_NO_INDICES);
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return expandWildcards.contains(WildcardStates.OPEN);
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return expandWildcards.contains(WildcardStates.CLOSED);
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return options.contains(Option.FORBID_CLOSED_INDICES);
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        out.writeEnumSet(options);
        out.writeEnumSet(expandWildcards);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        return new IndicesOptions(in.readEnumSet(Option.class), in.readEnumSet(WildcardStates.class));
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable,
                                             boolean allowNoIndices,
                                             boolean expandToOpenIndices,
                                             boolean expandToClosedIndices) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            false
        );
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable,
                                             boolean allowNoIndices,
                                             boolean expandToOpenIndices,
                                             boolean expandToClosedIndices,
                                             IndicesOptions defaultOptions) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            defaultOptions.forbidClosedIndices()
        );
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable,
                                             boolean allowNoIndices,
                                             boolean expandToOpenIndices,
                                             boolean expandToClosedIndices,
                                             boolean forbidClosedIndices) {
        final Set<Option> opts = new HashSet<>();
        final Set<WildcardStates> wildcards = new HashSet<>();

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
        if (forbidClosedIndices) {
            opts.add(Option.FORBID_CLOSED_INDICES);
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
        return builder;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     *         use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN;
    }

    /**
     * @return indices options that ignores unavailable indices,  expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpand() {
        return LENIENT_EXPAND_OPEN_CLOSED;
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
        return "IndicesOptions[" +
                "ignore_unavailable=" + ignoreUnavailable() +
                ", allow_no_indices=" + allowNoIndices() +
                ", expand_wildcards_open=" + expandWildcardsOpen() +
                ", expand_wildcards_closed=" + expandWildcardsClosed() +
                ", forbid_closed_indices=" + forbidClosedIndices() +
                ']';
    }
}
