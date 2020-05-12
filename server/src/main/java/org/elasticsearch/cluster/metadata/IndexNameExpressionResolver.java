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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

public class IndexNameExpressionResolver {

    private final WildcardExpressionResolver expressionResolver;

    public IndexNameExpressionResolver() {
        this.expressionResolver = new WildcardExpressionResolver();
    }

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        Context context = new Context(state, request.indicesOptions());
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        Context context = new Context(state, request.indicesOptions());
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
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options);
        return concreteIndexNames(context, indexExpressions);
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
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options, false, false);
        return concreteIndices(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param startTime         The start of the request where concrete indices is being invoked for
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, long startTime, String... indexExpressions) {
        Context context = new Context(state, options, startTime);
        return concreteIndices(context, indexExpressions);
    }

    String[] concreteIndexNames(Context context, String... indexExpressions) {
        Index[] indexes = concreteIndices(context, indexExpressions);
        String[] names = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            names[i] = indexes[i].getName();
        }
        return names;
    }

    Index[] concreteIndices(Context context, String... indexExpressions) {
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{MetaData.ALL};
        }
        MetaData metaData = context.getState().metaData();
        IndicesOptions options = context.getOptions();
        final boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        final boolean failNoIndices = indexExpressions.length == 1 ? !options.allowNoIndices() : !options.ignoreUnavailable();
        List<String> expressions = expressionResolver.resolve(context, Arrays.asList(indexExpressions));
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
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
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

            if (aliasOrIndex.isAlias() && context.isResolveToWriteIndex()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                IndexMetaData writeIndex = alias.getWriteIndex();
                if (writeIndex == null) {
                    throw new IllegalArgumentException("no write index is defined for alias [" + alias.getAliasName() + "]." +
                        " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple" +
                        " indices without one being designated as a write index");
                }
                concreteIndices.add(writeIndex.getIndex());
            } else {
                if (aliasOrIndex.getIndices().size() > 1 && !options.allowAliasesToMultipleIndices()) {
                    String[] indexNames = new String[aliasOrIndex.getIndices().size()];
                    int i = 0;
                    for (IndexMetaData indexMetaData : aliasOrIndex.getIndices()) {
                        indexNames[i++] = indexMetaData.getIndex().getName();
                    }
                    throw new IllegalArgumentException("Alias [" + expression + "] has more than one indices associated with it [" +
                        Arrays.toString(indexNames) + "], can't execute a single index op");
                }

                for (IndexMetaData index : aliasOrIndex.getIndices()) {
                    if (index.getState() == IndexMetaData.State.CLOSE) {
                        if (failClosed) {
                            throw new IndexClosedException(index.getIndex());
                        } else {
                            if (options.forbidClosedIndices() == false) {
                                concreteIndices.add(index.getIndex());
                            }
                        }
                    } else if (index.getState() == IndexMetaData.State.OPEN) {
                        concreteIndices.add(index.getIndex());
                    } else {
                        throw new IllegalStateException("index state [" + index.getState() + "] not supported");
                    }
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
    public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = request.indices() != null && request.indices().length > 0 ? request.indices()[0] : null;
        Index[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * @return the write index obtained as a result of the index resolution
     */
    public Index concreteWriteIndex(ClusterState state, IndicesRequest request) {
        if (request.indices() == null || (request.indices() != null && request.indices().length != 1)) {
            throw new IllegalArgumentException("indices request must specify a single index expression");
        }
        Context context = new Context(state, request.indicesOptions(), false, true);
        Index[] indices = concreteIndices(context, request.indices()[0]);
        if (indices.length != 1) {
            throw new IllegalArgumentException("The index expression [" + request.indices()[0] +
                "] and options provided did not point to a single write-index");
        }
        return indices[0];
    }

    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, Set<String> routing, String... expressions) {
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        List<String> resolvedExpressions = expressionResolver.resolve(
            context,
            expressions != null ? Arrays.asList(expressions) : Collections.emptyList()
        );
        // TODO: it appears that this can never be true?
        if (isAllIndices(resolvedExpressions)) {
            return resolveSearchRoutingAllIndices(state.metaData(), routing);
        }

        Map<String, Set<String>> routings = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();

        for (String expression : resolvedExpressions) {
            AliasOrIndex aliasOrIndex = state.metaData().getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                for (Tuple<String, AliasMetaData> item : alias.getConcreteIndexAndAliasMetaDatas()) {
                    String concreteIndex = item.v1();
                    AliasMetaData aliasMetaData = item.v2();
                    if (!norouting.contains(concreteIndex)) {
                        if (!aliasMetaData.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.get(concreteIndex);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(concreteIndex, r);
                            }
                            r.addAll(aliasMetaData.searchRoutingValues());
                            if (!routing.isEmpty()) {
                                r.retainAll(routing);
                            }
                            if (r.isEmpty()) {
                                routings.remove(concreteIndex);
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(concreteIndex)) {
                                norouting.add(concreteIndex);
                                if (!routing.isEmpty()) {
                                    Set<String> r = new HashSet<>(routing);
                                    if (routings == null) {
                                        routings = new HashMap<>();
                                    }
                                    routings.put(concreteIndex, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(concreteIndex);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Index
                if (!norouting.contains(expression)) {
                    norouting.add(expression);
                    if (!routing.isEmpty()) {
                        Set<String> r = new HashSet<>(routing);
                        if (routings == null) {
                            routings = new HashMap<>();
                        }
                        routings.put(expression, r);
                    } else {
                        if (routings != null) {
                            routings.remove(expression);
                        }
                    }
                }
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     */
    private Map<String, Set<String>> resolveSearchRoutingAllIndices(MetaData metaData, Set<String> routing) {
        if (!routing.isEmpty()) {
            Map<String, Set<String>> routings = new HashMap<>();
            String[] concreteIndices = metaData.getConcreteAllIndices();
            for (String index : concreteIndices) {
                routings.put(index, routing);
            }
            return routings;
        }
        return null;
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
        return aliasesOrIndices != null && aliasesOrIndices.size() == 1 && MetaData.ALL.equals(aliasesOrIndices.get(0));
    }

    /**
     * Identifies whether the first argument (an array containing index names) is a pattern that matches all indices
     *
     * @param indicesOrAliases the array containing index names
     * @param concreteIndices  array containing the concrete indices that the first argument refers to
     * @return true if the first argument is a pattern that maps to all available indices, false otherwise
     */
    boolean isPatternMatchingAllIndices(MetaData metaData, String[] indicesOrAliases, String[] concreteIndices) {
        // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
        if (concreteIndices.length == metaData.getConcreteAllIndices().length && indicesOrAliases.length > 0) {

            //we might have something like /-test1,+test1 that would identify all indices
            //or something like /-test1 with test1 index missing and IndicesOptions.lenient()
            if (indicesOrAliases[0].charAt(0) == '-') {
                return true;
            }

            //otherwise we check if there's any simple regex
            for (String indexOrAlias : indicesOrAliases) {
                if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                    return true;
                }
            }
        }
        return false;
    }

    static final class Context {

        private final ClusterState state;
        private final IndicesOptions options;
        private final long startTime;
        private final boolean preserveAliases;
        private final boolean resolveToWriteIndex;

        Context(ClusterState state, IndicesOptions options) {
            this(state, options, System.currentTimeMillis());
        }

        Context(ClusterState state, IndicesOptions options, boolean preserveAliases, boolean resolveToWriteIndex) {
            this(state, options, System.currentTimeMillis(), preserveAliases, resolveToWriteIndex);
        }

        Context(ClusterState state, IndicesOptions options, long startTime) {
            this(state, options, startTime, false, false);
        }

        Context(ClusterState state, IndicesOptions options, long startTime, boolean preserveAliases, boolean resolveToWriteIndex) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
            this.preserveAliases = preserveAliases;
            this.resolveToWriteIndex = resolveToWriteIndex;
        }

        public ClusterState getState() {
            return state;
        }

        public IndicesOptions getOptions() {
            return options;
        }

        public long getStartTime() {
            return startTime;
        }

        /**
         * This is used to prevent resolving aliases to concrete indices but this also means
         * that we might return aliases that point to a closed index. This is currently only used
         * by {@link #filteringAliases(ClusterState, String, String...)} since it's the only one that needs aliases
         */
        boolean isPreserveAliases() {
            return preserveAliases;
        }

        /**
         * This is used to require that aliases resolve to their write-index. It is currently not used in conjunction
         * with <code>preserveAliases</code>.
         */
        boolean isResolveToWriteIndex() {
            return resolveToWriteIndex;
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
            MetaData metaData = context.getState().metaData();
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            if (isEmptyOrTrivialWildcard(expressions)) {
                return resolveEmptyOrTrivialWildcard(options, metaData);
            }

            Set<String> result = innerResolve(context, expressions, options, metaData);

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

        private Set<String> innerResolve(Context context, List<String> expressions, IndicesOptions options, MetaData metaData) {
            Set<String> result = null;
            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                if (Strings.isEmpty(expression)) {
                    throw indexNotFoundException(expression);
                }
                validateAliasOrIndex(expression);
                if (aliasOrIndexExists(options, metaData, expression)) {
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
                        AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
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

                final IndexMetaData.State excludeState = excludeState(options);
                final Map<String, AliasOrIndex> matches = matches(context, metaData, expression);
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

        private static void validateAliasOrIndex(String expression) {
            // Expressions can not start with an underscore. This is reserved for APIs. If the check gets here, the API
            // does not exist and the path is interpreted as an expression. If the expression begins with an underscore,
            // throw a specific error that is different from the [[IndexNotFoundException]], which is typically thrown
            // if the expression can't be found.
            if (expression.charAt(0) == '_') {
                throw new InvalidIndexNameException(expression, "must not start with '_'.");
            }
        }

        private static boolean aliasOrIndexExists(IndicesOptions options, MetaData metaData, String expression) {
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
            //treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            return aliasOrIndex != null && (options.ignoreAliases() == false || aliasOrIndex.isAlias() == false);
        }

        private static IndexNotFoundException indexNotFoundException(String expression) {
            IndexNotFoundException infe = new IndexNotFoundException(expression);
            infe.setResources("index_or_alias", expression);
            return infe;
        }

        private static IndexMetaData.State excludeState(IndicesOptions options) {
            final IndexMetaData.State excludeState;
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                excludeState = null;
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
                excludeState = IndexMetaData.State.CLOSE;
            } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
                excludeState = IndexMetaData.State.OPEN;
            } else {
                assert false : "this shouldn't get called if wildcards expand to none";
                excludeState = null;
            }
            return excludeState;
        }

        public static Map<String, AliasOrIndex> matches(Context context, MetaData metaData, String expression) {
            if (Regex.isMatchAllPattern(expression)) {
                // Can only happen if the expressions was initially: '-*'
                if (context.getOptions().ignoreAliases()) {
                    return metaData.getAliasAndIndexLookup().entrySet().stream()
                            .filter(e -> e.getValue().isAlias() == false)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                } else {
                    return metaData.getAliasAndIndexLookup();
                }
            } else if (expression.indexOf("*") == expression.length() - 1) {
                return suffixWildcard(context, metaData, expression);
            } else {
                return otherWildcard(context, metaData, expression);
            }
        }

        private static Map<String, AliasOrIndex> suffixWildcard(Context context, MetaData metaData, String expression) {
            assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
            String fromPrefix = expression.substring(0, expression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            SortedMap<String,AliasOrIndex> subMap = metaData.getAliasAndIndexLookup().subMap(fromPrefix, toPrefix);
            if (context.getOptions().ignoreAliases()) {
                return subMap.entrySet().stream()
                    .filter(entry -> entry.getValue().isAlias() == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return subMap;
        }

        private static Map<String, AliasOrIndex> otherWildcard(Context context, MetaData metaData, String expression) {
            final String pattern = expression;
            return metaData.getAliasAndIndexLookup()
                .entrySet()
                .stream()
                .filter(e -> context.getOptions().ignoreAliases() == false || e.getValue().isAlias() == false)
                .filter(e -> Regex.simpleMatch(pattern, e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private static Set<String> expand(Context context, IndexMetaData.State excludeState, Map<String, AliasOrIndex> matches) {
            Set<String> expand = new HashSet<>();
            for (Map.Entry<String, AliasOrIndex> entry : matches.entrySet()) {
                AliasOrIndex aliasOrIndex = entry.getValue();
                if (context.isPreserveAliases() && aliasOrIndex.isAlias()) {
                    expand.add(entry.getKey());
                } else {
                    for (IndexMetaData meta : aliasOrIndex.getIndices()) {
                        if (excludeState == null || meta.getState() != excludeState) {
                            expand.add(meta.getIndex().getName());
                        }
                    }
                }
            }
            return expand;
        }

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
            return expressions.isEmpty() || (expressions.size() == 1 && (MetaData.ALL.equals(expressions.get(0)) || Regex.isMatchAllPattern(expressions.get(0))));
        }

        private static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, MetaData metaData) {
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                return Arrays.asList(metaData.getConcreteAllIndices());
            } else if (options.expandWildcardsOpen()) {
                return Arrays.asList(metaData.getConcreteAllOpenIndices());
            } else if (options.expandWildcardsClosed()) {
                return Arrays.asList(metaData.getConcreteAllClosedIndices());
            } else {
                return Collections.emptyList();
            }
        }
    }
}
