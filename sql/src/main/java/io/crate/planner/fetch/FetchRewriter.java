/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public final class FetchRewriter {

    /**
     * Use the fetchDescription to generate a List of symbols which contain symbols in a format that can be
     * used by {@link io.crate.planner.projection.FetchProjection}
     *
     * Example:
     * <pre>
     *     fetchDescription
     *          preFetchOutputs: [_fetchId, x]
     *          postFetchOutputs: [z, y, x + x]
     *
     *     result:
     *         [FetchRef(ic0, z), FetchRef(ic0, y), ic1 + ic1]
     *
     * </pre>
     */
    public static List<Symbol> generateFetchOutputs(FetchDescription fetchDescription) {
        InputColumn fetchId = new InputColumn(
            fetchDescription.preFetchOutputs.indexOf(fetchDescription.fetchId), fetchDescription.fetchId.valueType());
        return Lists2.copyAndReplace(
            fetchDescription.postFetchOutputs,
            st -> toFetchReferenceOrInputColumn(st, fetchDescription.preFetchOutputs, fetchId));
    }

    /**
     * Replace {@code tree} or any reference within it with a {@link InputColumn} if present in {@code preFetchOutputs}.
     *
     * Any references within {@code tree} that are not available until after the fetch phase
     * will be replaced with a {@link FetchReference}
     */
    private static Symbol toFetchReferenceOrInputColumn(Symbol tree,
                                                        List<? extends Symbol> preFetchOutputs,
                                                        InputColumn fetchId) {
        int indexOf = preFetchOutputs.indexOf(tree);
        if (indexOf == -1) {
            return RefReplacer.replaceRefs(tree, r -> {
                int i = preFetchOutputs.indexOf(r);
                if (i == -1) {
                    return new FetchReference(fetchId, r);
                }
                return new InputColumn(i, preFetchOutputs.get(i).valueType());
            });
        }
        return new InputColumn(indexOf, preFetchOutputs.get(indexOf).valueType());
    }

    public static final class FetchDescription {

        private final TableIdent tableIdent;
        private final List<Reference> partitionedByColumns;
        private final Reference fetchId;
        private final List<Symbol> preFetchOutputs;

        /**
          * Symbols describing the outputs after the fetch.
          *
          * These are the columns from the selectList/querySpec#outputs but may have been modified
          * (For example to do a source lookup)
          *
          * The order of the column matches the order they had in the selectList/querySpec
          */
        final List<Symbol> postFetchOutputs;
        private final Collection<Reference> fetchRefs;

        private FetchDescription(TableIdent tableIdent,
                                 List<Reference> partitionedByColumns,
                                 Reference fetchId,
                                 List<Symbol> preFetchOutputs,
                                 List<Symbol> postFetchOutputs,
                                 Collection<Reference> fetchRefs) {
            this.tableIdent = tableIdent;
            this.partitionedByColumns = partitionedByColumns;
            this.fetchId = fetchId;
            this.preFetchOutputs = preFetchOutputs;
            this.postFetchOutputs = postFetchOutputs;
            this.fetchRefs = fetchRefs;
        }

        public Collection<Reference> fetchRefs() {
            return fetchRefs;
        }

        public List<Reference> partitionedByColumns() {
            return partitionedByColumns;
        }

        public TableIdent table() {
            return tableIdent;
        }

        public Collection<? extends Symbol> preFetchOutputs() {
            return preFetchOutputs;
        }

        /**
         * Test if any Fields within {@code tree} are available pre-fetch.
         *
         */
        public boolean availablePreFetch(Symbol tree) {
            boolean[] available = new boolean[] { true };
            FieldsVisitor.visitFields(tree, f -> {
                Symbol symbol = mapFieldToPostFetchOutput(f);
                if (preFetchOutputs.contains(symbol)) {
                    available[0] &= true;
                } else {
                    RefVisitor.visitRefs(symbol, r -> {
                        available[0] &= preFetchOutputs.contains(r);
                    });
                }
            });
            return available[0];
        }

        public boolean availablePreFetch(Iterable<? extends Symbol> trees) {
            for (Symbol tree : trees) {
                if (!availablePreFetch(tree)) {
                    return false;
                }
            }
            return true;
        }

        public Function<? super Symbol,? extends Symbol> mapFieldsInTreeToPostFetch() {
            return FieldReplacer.bind(this::mapFieldToPostFetchOutput);
        }

        /**
         * Update the postFetchOutput to include any evaluations from the selectList of a parent relation.
         *
         * Example:
         * <pre>
         *     select x + x, y from (select y, x from t order by x) tt
         *                           preFetch: Ref[_fetchId], Ref[x]
         *                           postFetch: Ref[_docY], Ref[x]
         *            updatePostFetchOutputs add(Field[x, idx=1], Field[x, idx=1]), Ref[_docY]
         *            ->
         *            postFetch: add(Ref[x], Ref[x]), Ref[_docY]
         * </pre>
         */
        public void updatePostFetchOutputs(List<Symbol> newOutputs) {
            List<Symbol> newPostFetchOutputs = Lists2.copyAndReplace(
                newOutputs,
                st -> FieldReplacer.replaceFields(st, this::mapFieldToPostFetchOutput));
            postFetchOutputs.clear();
            postFetchOutputs.addAll(newPostFetchOutputs);
        }


        private Symbol mapFieldToPostFetchOutput(Field field) {
            return postFetchOutputs.get(field.index());
        }
    }

    public static boolean isFetchFeasible(QuerySpec querySpec) {
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);
        return FetchFeasibility.isFetchFeasible(querySpec.outputs(), querySymbols);
    }

    private static Set<Symbol> extractQuerySymbols(QuerySpec querySpec) {
        Optional<OrderBy> orderBy = querySpec.orderBy();
        return orderBy.isPresent()
            ? ImmutableSet.copyOf(orderBy.get().orderBySymbols())
            : ImmutableSet.of();
    }

    public static FetchDescription rewrite(QueriedDocTable query) {
        QuerySpec querySpec = query.querySpec();
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);

        assert FetchFeasibility.isFetchFeasible(querySpec.outputs(), querySymbols)
            : "Fetch rewrite shouldn't be done if it's not feasible";


        DocTableInfo tableInfo = query.tableRelation().tableInfo();
        Reference fetchId = DocSysColumns.forTable(tableInfo.ident(), DocSysColumns.FETCHID);
        ArrayList<Symbol> preFetchOutputs = new ArrayList<>(1 + querySymbols.size());
        preFetchOutputs.add(fetchId);
        preFetchOutputs.addAll(querySymbols);

        SymbolForFetchConverter symbolForFetchConverter = new SymbolForFetchConverter(querySymbols);
        List<Symbol> postFetchOutputs = Lists2.copyAndReplace(querySpec.outputs(), symbolForFetchConverter);

        Reference scoreRef = symbolForFetchConverter.scoreRef;
        if (scoreRef != null) {
            preFetchOutputs.add(scoreRef);
        }
        querySpec.outputs(preFetchOutputs);
        return new FetchDescription(
            tableInfo.ident(),
            tableInfo.partitionedByColumns(),
            fetchId,
            preFetchOutputs,
            postFetchOutputs,
            symbolForFetchConverter.fetchRefs);
    }


    /**
     * Function to replace output to be suitable for post-fetch:
     *
     *  - Converts references to do a source lookup if possible
     *  - Keeps a reference to a _score Reference if it is encountered (_score can't be fetched)
     *  - Adds references that will need to be fetched to {@link SymbolForFetchConverter#fetchRefs}
     */
    private static final class SymbolForFetchConverter implements Function<Symbol, Symbol> {

        private final Set<Symbol> querySymbols;
        private final Set<Reference> fetchRefs = new LinkedHashSet<>();

        private Reference scoreRef = null;

        SymbolForFetchConverter(Set<Symbol> querySymbols) {
            this.querySymbols = querySymbols;
        }

        @Override
        public Symbol apply(Symbol symbol) {
            if (querySymbols.contains(symbol)) {
                return symbol;
            }
            return RefReplacer.replaceRefs(symbol, this::maybeConvertToSourceLookupAndAddToFetch);
        }

        private Symbol maybeConvertToSourceLookupAndAddToFetch(Reference ref) {
            if (ref.granularity() == RowGranularity.DOC) {
                if (ref.ident().columnIdent().equals(DocSysColumns.SCORE)) {
                    scoreRef = ref;
                    return ref;
                }
                if (querySymbols.contains(ref)) {
                    return ref;
                }
                Reference reference = DocReferences.toSourceLookup(ref);
                fetchRefs.add(reference);
                return reference;
            }
            return ref;
        }
    }
}
