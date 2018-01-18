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

package io.crate.planner;

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.analyze.GeneratedColumnExpander;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.EqualityExtractor;
import io.crate.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * Used to analyze a query for primaryKey/partition "direct access" possibilities.
 *
 * This is similar to {@link io.crate.analyze.where.WhereClauseAnalyzer} - the difference is that this works also if
 * the query still contains ParameterSymbols.
 *
 * Once all Analyzers are migrated to be "unbound",
 * the WhereClauseAnalyzer should be removed (or reworked, to just do the final partition selection / tie-breaking)
 */
public final class WhereClauseOptimizer {

    private WhereClauseOptimizer() {
    }

    public static class DetailedQuery {

        private final Symbol query;
        private final DocKeys docKeys;
        private final List<List<Symbol>> partitionValues;
        private final Set<Symbol> clusteredByValues;

        DetailedQuery(Symbol query,
                      DocKeys docKeys,
                      List<List<Symbol>> partitionValues,
                      @Nullable Set<Symbol> clusteredByValues) {
            this.query = query;
            this.docKeys = docKeys;
            this.partitionValues = firstNonNull(partitionValues, Collections.emptyList());
            this.clusteredByValues = clusteredByValues;
        }

        public Optional<DocKeys> docKeys() {
            return Optional.ofNullable(docKeys);
        }

        /**
         * @return Symbols "pointing" to the values of any `partition_col = S` expressions:
         *         The outer list contains 1 entry per "equals pair" (e.g. `pcol = ? or pcol = ?` -> 2 entries
         *
         *         The inner list contains 1 entry per partitioned by column.
         *         The order matches the order of the partitioned by column definition.
         */
        public List<List<Symbol>> partitions() {
            return partitionValues;
        }

        public Symbol query() {
            return query;
        }

        @Nullable
        public Set<Symbol> clusteredBy() {
            return clusteredByValues;
        }
    }

    public static DetailedQuery optimize(EvaluatingNormalizer normalizer,
                                         Symbol query,
                                         DocTableInfo table,
                                         TransactionContext txnCtx) {
        Symbol queryGenColsProcessed = GeneratedColumnExpander.maybeExpand(
            query,
            table.generatedColumns(),
            Lists2.concat(table.partitionedByColumns(), Lists2.copyAndReplace(table.primaryKey(), table::getReference)));
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, txnCtx);
        }

        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        List<ColumnIdent> pkCols = pkColsInclVersion(table, versionInQuery);

        EqualityExtractor eqExtractor = new EqualityExtractor(normalizer);
        List<List<Symbol>> pkValues = eqExtractor.extractExactMatches(pkCols, query, txnCtx);

        int clusterdIdxWithinPK = table.primaryKey().indexOf(table.clusteredBy());
        final DocKeys docKeys;
        if (pkValues == null) {
            docKeys = null;
        } else {
            List<Integer> partitionIndicesWithinPks = null;
            if (table.isPartitioned()) {
                partitionIndicesWithinPks = getPartitionIndices(table.primaryKey(), table.partitionedBy());
            }
            docKeys = new DocKeys(pkValues, versionInQuery, clusterdIdxWithinPK, partitionIndicesWithinPks);
        }
        List<List<Symbol>> partitionValues = null;
        if (table.isPartitioned()) {
            partitionValues = eqExtractor.extractExactMatches(table.partitionedBy(), query, txnCtx);
        }
        Set<Symbol> clusteredBy = null;
        if (table.clusteredBy() != null) {
            List<List<Symbol>> clusteredByValues = eqExtractor.extractParentMatches(
                Collections.singletonList(table.clusteredBy()), query, txnCtx);
            if (clusteredByValues != null) {
                clusteredBy = new HashSet<>(clusteredByValues.size());
                for (List<Symbol> s : clusteredByValues) {
                    clusteredBy.add(s.get(0));
                }
            }
        }
        return new DetailedQuery(query, docKeys, partitionValues, clusteredBy);
    }

    public static List<Integer> getPartitionIndices(List<ColumnIdent> pkCols, List<ColumnIdent> partitionCols) {
        ArrayList<Integer> result = new ArrayList<>(partitionCols.size());
        for (int i = 0; i < partitionCols.size(); i++) {
            ColumnIdent partitionCol = partitionCols.get(i);
            int partColIdxInPks = pkCols.indexOf(partitionCol);
            if (partColIdxInPks >= 0) {
                result.add(partColIdxInPks);
            }
        }
        return result;
    }

    private static List<ColumnIdent> pkColsInclVersion(DocTableInfo table, boolean versionInQuery) {
        if (versionInQuery) {
            ArrayList<ColumnIdent> pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
            return pkCols;
        }
        return table.primaryKey();
    }
}
