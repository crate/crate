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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;

public class IndexNameExpressionResolver {

    private static final WildcardExpressionResolver EXPRESSION_RESOLVER = new WildcardExpressionResolver();

    private IndexNameExpressionResolver() {
    }


    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public static String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        Context context = new Context(state.metadata(), request.indicesOptions());
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public static Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        Context context = new Context(state.metadata(), request.indicesOptions());
        return concreteIndices(context, request.indices());
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     */
    public static String[] concreteIndexNames(Metadata metadata, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(metadata, options);
        return concreteIndexNames(context, indexExpressions);
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
     */
    public static Index[] concreteIndices(Metadata metadata, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(metadata, options);
        return concreteIndices(context, indexExpressions);
    }

    static String[] concreteIndexNames(Context context, String... indexExpressions) {
        Index[] indexes = concreteIndices(context, indexExpressions);
        String[] names = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            names[i] = indexes[i].getName();
        }
        return names;
    }

    static Index[] concreteIndices(Context context, String... indexExpressions) {
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{Metadata.ALL};
        }
        Metadata metadata = context.metadata();
        IndicesOptions options = context.getOptions();
        final boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        final boolean failNoIndices = indexExpressions.length == 1 ? !options.allowNoIndices() : !options.ignoreUnavailable();
        List<String> expressions = EXPRESSION_RESOLVER.resolve(context, Arrays.asList(indexExpressions));
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
            } else if (aliasOrIndex.isAlias() && context.getOptions().ignoreAliases()) {
                if (failNoIndices) {
                    throw aliasesNotSupportedException(expression);
                } else {
                    continue;
                }
            }

            if (aliasOrIndex.getIndices().size() > 1 && !options.allowAliasesToMultipleIndices()) {
                String[] indexNames = new String[aliasOrIndex.getIndices().size()];
                int i = 0;
                for (IndexMetadata indexMetadata : aliasOrIndex.getIndices()) {
                    indexNames[i++] = indexMetadata.getIndex().getName();
                }
                throw new IllegalArgumentException("Alias [" + expression + "] has more than one indices associated with it [" +
                    Arrays.toString(indexNames) + "], can't execute a single index op");
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

    private static IllegalArgumentException aliasesNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches an " +
                "alias, specify the corresponding concrete indices instead.");
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link IllegalArgumentException} gets thrown otherwise.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution lead to more than one index
     * @return the concrete index obtained as a result of the index resolution
     */
    public static Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = request.indices().length == 0 ? null : request.indices()[0];
        String[] indexExpressions = { indexExpression };
        Index[] indices = concreteIndices(state.metadata(), request.indicesOptions(), indexExpressions);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(List<String> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(List<String> aliasesOrIndices) {
        return aliasesOrIndices != null && aliasesOrIndices.size() == 1 && Metadata.ALL.equals(aliasesOrIndices.get(0));
    }

    static final class Context {

        private final Metadata metadata;
        private final IndicesOptions options;

        Context(Metadata metadata, IndicesOptions options) {
            this.metadata = metadata;
            this.options = options;
        }

        public Metadata metadata() {
            return metadata;
        }

        public IndicesOptions getOptions() {
            return options;
        }
    }

    private interface ExpressionResolver {

        /**
         * Resolves the list of expressions into other expressions if possible (possible concrete indices and aliases, but
         * that isn't required). The provided implementations can also be left untouched.
         *
         * @return a new list with expressions based on the provided expressions
         */
        List<String> resolve(Context context, List<String> expressions);

    }

    /**
     * Resolves alias/index name expressions with wildcards into the corresponding concrete indices/aliases
     */
    static final class WildcardExpressionResolver implements ExpressionResolver {

        @Override
        public List<String> resolve(Context context, List<String> expressions) {
            IndicesOptions options = context.getOptions();
            Metadata metadata = context.metadata();
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            if (isEmptyOrTrivialWildcard(expressions)) {
                return resolveEmptyOrTrivialWildcard(options, metadata);
            }

            Set<String> result = innerResolve(context, expressions, options, metadata);

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

        private Set<String> innerResolve(Context context, List<String> expressions, IndicesOptions options, Metadata metadata) {
            Set<String> result = null;
            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                if (Strings.isNullOrEmpty(expression)) {
                    throw indexNotFoundException(expression);
                }
                if (aliasOrIndexExists(options, metadata, expression)) {
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
                        } else if (aliasOrIndex.isAlias() && options.ignoreAliases()) {
                            throw aliasesNotSupportedException(expression);
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
                final Map<String, AliasOrIndex> matches = matches(context, metadata, expression);
                Set<String> expand = expand(context, excludeState, matches);
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

        private static boolean aliasOrIndexExists(IndicesOptions options, Metadata metadata, String expression) {
            AliasOrIndex aliasOrIndex = metadata.getAliasAndIndexLookup().get(expression);
            //treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            return aliasOrIndex != null && (options.ignoreAliases() == false || aliasOrIndex.isAlias() == false);
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

        public static Map<String, AliasOrIndex> matches(Context context, Metadata metadata, String expression) {
            if (Regex.isMatchAllPattern(expression)) {
                // Can only happen if the expressions was initially: '-*'
                if (context.getOptions().ignoreAliases()) {
                    return metadata.getAliasAndIndexLookup().entrySet().stream()
                            .filter(e -> e.getValue().isAlias() == false)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                } else {
                    return metadata.getAliasAndIndexLookup();
                }
            } else if (expression.indexOf("*") == expression.length() - 1) {
                return suffixWildcard(context, metadata, expression);
            } else {
                return otherWildcard(context, metadata, expression);
            }
        }

        private static Map<String, AliasOrIndex> suffixWildcard(Context context, Metadata metadata, String expression) {
            assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
            String fromPrefix = expression.substring(0, expression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            SortedMap<String,AliasOrIndex> subMap = metadata.getAliasAndIndexLookup().subMap(fromPrefix, toPrefix);
            if (context.getOptions().ignoreAliases()) {
                return subMap.entrySet().stream()
                    .filter(entry -> entry.getValue().isAlias() == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return subMap;
        }

        private static Map<String, AliasOrIndex> otherWildcard(Context context, Metadata metadata, String expression) {
            final String pattern = expression;
            return metadata.getAliasAndIndexLookup()
                .entrySet()
                .stream()
                .filter(e -> context.getOptions().ignoreAliases() == false || e.getValue().isAlias() == false)
                .filter(e -> Regex.simpleMatch(pattern, e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private static Set<String> expand(Context context, IndexMetadata.State excludeState, Map<String, AliasOrIndex> matches) {
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

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
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
}
