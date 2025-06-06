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

package org.elasticsearch.cluster.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;

public class IndexNameExpressionResolver {

    private IndexNameExpressionResolver() {
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param metadata          the cluster state metadata containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     *
     * @deprecated indices should be resolved via {@link Metadata#getIndices(io.crate.metadata.RelationName, List, boolean, java.util.function.Function)
     */
    @Deprecated
    public static String[] concreteIndexNames(Metadata metadata, IndicesOptions options, String... indexExpressions) {
        Index[] indexes = concreteIndices(metadata, options, indexExpressions);
        String[] names = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            names[i] = indexes[i].getName();
        }
        return names;
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param metadata          metadata from the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     *
     * @deprecated indices should be resolved via {@link Metadata#getIndices(io.crate.metadata.RelationName, List, boolean, java.util.function.Function)
     */
    @Deprecated
    public static Index[] concreteIndices(Metadata metadata, IndicesOptions options, String... indexExpressions) {
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{Metadata.ALL};
        }
        final boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        final boolean failNoIndices = indexExpressions.length == 1 ? !options.allowNoIndices() : !options.ignoreUnavailable();
        List<String> expressions = resolve(metadata, options, Arrays.asList(indexExpressions));
        if (expressions.isEmpty()) {
            if (!options.allowNoIndices()) {
                IndexNotFoundException infe = new IndexNotFoundException((String)null);
                infe.setResources("index_expression", indexExpressions);
                throw infe;
            } else {
                return Index.EMPTY_ARRAY;
            }
        }

        final Set<Index> concreteIndices = new HashSet<>(expressions.size());
        for (String expression : expressions) {
            AliasOrIndex aliasOrIndex = metadata.getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex == null) {
                if (failNoIndices) {
                    IndexNotFoundException infe = new IndexNotFoundException(expression);
                    infe.setResources("index_expression", expression);
                    throw infe;
                } else {
                    continue;
                }
            }
            for (IndexMetadata index : aliasOrIndex.getIndices()) {
                if (index.getState() == IndexMetadata.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(index.getIndex());
                    } else {
                        if (options.forbidClosedIndices() == false) {
                            concreteIndices.add(index.getIndex());
                        }
                    }
                } else if (index.getState() == IndexMetadata.State.OPEN) {
                    concreteIndices.add(index.getIndex());
                } else {
                    throw new IllegalStateException("index state [" + index.getState() + "] not supported");
                }
            }
        }

        if (options.allowNoIndices() == false && concreteIndices.isEmpty()) {
            IndexNotFoundException infe = new IndexNotFoundException((String)null);
            infe.setResources("index_expression", indexExpressions);
            throw infe;
        }
        return concreteIndices.toArray(new Index[concreteIndices.size()]);
    }


    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(List<String> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || Metadata.ALL.equals(aliasesOrIndices.get(0));
    }

    /**
     * Resolves the list of expressions into other expressions if possible (possible concrete indices and aliases, but
     * that isn't required). The provided implementations can also be left untouched.
     *
     * @return a new list with expressions based on the provided expressions
     */
    private static List<String> resolve(Metadata metadata, IndicesOptions options, List<String> expressions) {
        if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
            return expressions;
        }

        if (isEmptyOrTrivialWildcard(expressions)) {
            return resolveEmptyOrTrivialWildcard(options, metadata);
        }

        Set<String> result = innerResolve(metadata, options, expressions);

        if (result == null) {
            return expressions;
        }
        if (result.isEmpty() && !options.allowNoIndices()) {
            IndexNotFoundException infe = new IndexNotFoundException((String)null);
            infe.setResources("index_or_alias", expressions.toArray(new String[0]));
            throw infe;
        }
        return new ArrayList<>(result);
    }

    private static Set<String> innerResolve(Metadata metadata, IndicesOptions options, List<String> expressions) {
        Set<String> result = null;
        boolean wildcardSeen = false;
        for (int i = 0; i < expressions.size(); i++) {
            String expression = expressions.get(i);
            if (Strings.isNullOrEmpty(expression)) {
                throw indexNotFoundException(expression);
            }
            if (aliasOrIndexExists(metadata, options, expression)) {
                if (result != null) {
                    result.add(expression);
                }
                continue;
            }
            final boolean add;
            if (expression.charAt(0) == '-' && wildcardSeen) {
                add = false;
                expression = expression.substring(1);
            } else {
                add = true;
            }
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>(expressions.subList(0, i));
            }
            if (!Regex.isSimpleMatchPattern(expression)) {
                //TODO why does wildcard resolver throw exceptions regarding non wildcarded expressions? This should not be done here.
                if (options.ignoreUnavailable() == false) {
                    AliasOrIndex aliasOrIndex = metadata.getAliasAndIndexLookup().get(expression);
                    if (aliasOrIndex == null) {
                        throw indexNotFoundException(expression);
                    }
                }
                if (add) {
                    result.add(expression);
                } else {
                    result.remove(expression);
                }
                continue;
            }

            final IndexMetadata.State excludeState = excludeState(options);
            final Map<String, AliasOrIndex> matches = matches(metadata, options, expression);
            Set<String> expand = expand(excludeState, matches);
            if (add) {
                result.addAll(expand);
            } else {
                result.removeAll(expand);
            }
            if (options.allowNoIndices() == false && matches.isEmpty()) {
                throw indexNotFoundException(expression);
            }
            if (Regex.isSimpleMatchPattern(expression)) {
                wildcardSeen = true;
            }
        }
        return result;
    }

    private static boolean aliasOrIndexExists(Metadata metadata, IndicesOptions options, String expression) {
        AliasOrIndex aliasOrIndex = metadata.getAliasAndIndexLookup().get(expression);
        return aliasOrIndex != null;
    }

    private static IndexNotFoundException indexNotFoundException(String expression) {
        IndexNotFoundException infe = new IndexNotFoundException(expression);
        infe.setResources("index_or_alias", expression);
        return infe;
    }

    private static IndexMetadata.State excludeState(IndicesOptions options) {
        final IndexMetadata.State excludeState;
        if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
            excludeState = null;
        } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
            excludeState = IndexMetadata.State.CLOSE;
        } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
            excludeState = IndexMetadata.State.OPEN;
        } else {
            assert false : "this shouldn't get called if wildcards expand to none";
            excludeState = null;
        }
        return excludeState;
    }

    private static Map<String, AliasOrIndex> matches(Metadata metadata, IndicesOptions options, String expression) {
        if (Regex.isMatchAllPattern(expression)) {
            return metadata.getAliasAndIndexLookup();
        } else if (expression.indexOf("*") == expression.length() - 1) {
            return suffixWildcard(metadata, options, expression);
        } else {
            return otherWildcard(metadata, options, expression);
        }
    }

    private static Map<String, AliasOrIndex> suffixWildcard(Metadata metadata, IndicesOptions options, String expression) {
        assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
        String fromPrefix = expression.substring(0, expression.length() - 1);
        char[] toPrefixCharArr = fromPrefix.toCharArray();
        toPrefixCharArr[toPrefixCharArr.length - 1]++;
        String toPrefix = new String(toPrefixCharArr);
        SortedMap<String,AliasOrIndex> subMap = metadata.getAliasAndIndexLookup().subMap(fromPrefix, toPrefix);
        return subMap;
    }

    private static Map<String, AliasOrIndex> otherWildcard(Metadata metadata, IndicesOptions options, String expression) {
        final String pattern = expression;
        return metadata.getAliasAndIndexLookup()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().isAlias() == false)
            .filter(e -> Regex.simpleMatch(pattern, e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Set<String> expand(IndexMetadata.State excludeState, Map<String, AliasOrIndex> matches) {
        Set<String> expand = new HashSet<>();
        for (Map.Entry<String, AliasOrIndex> entry : matches.entrySet()) {
            AliasOrIndex aliasOrIndex = entry.getValue();
            for (IndexMetadata meta : aliasOrIndex.getIndices()) {
                if (excludeState == null || meta.getState() != excludeState) {
                    expand.add(meta.getIndex().getName());
                }
            }
        }
        return expand;
    }

    private static boolean isEmptyOrTrivialWildcard(List<String> expressions) {
        return expressions.isEmpty() || (expressions.size() == 1 && (Metadata.ALL.equals(expressions.get(0)) || Regex.isMatchAllPattern(expressions.get(0))));
    }

    private static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, Metadata metadata) {
        if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
            return Arrays.asList(metadata.getConcreteAllIndices());
        } else if (options.expandWildcardsOpen()) {
            return Arrays.asList(metadata.getConcreteAllOpenIndices());
        } else if (options.expandWildcardsClosed()) {
            return Arrays.asList(metadata.getConcreteAllClosedIndices());
        } else {
            return Collections.emptyList();
        }
    }
}
