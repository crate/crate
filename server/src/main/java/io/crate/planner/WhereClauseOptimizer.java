/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.WhereClause;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.EqualityExtractor;
import io.crate.analyze.where.EqualityExtractor.EqMatches;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

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
        private final List<List<Symbol>> partitions;
        private final Set<Symbol> clusteredByValues;
        private final boolean queryHasPkSymbolsOnly;

        DetailedQuery(Symbol query,
                      DocKeys docKeys,
                      List<List<Symbol>> partitionValues,
                      Set<Symbol> clusteredByValues,
                      DocTableInfo table,
                      boolean queryHasPkSymbolsOnly) {
            this.query = query;
            this.docKeys = docKeys;
            this.partitions = Objects.requireNonNullElse(partitionValues, Collections.emptyList());
            this.clusteredByValues = clusteredByValues;
            this.queryHasPkSymbolsOnly = queryHasPkSymbolsOnly;
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
            return partitions;
        }

        public Symbol query() {
            return query;
        }

        public Set<Symbol> clusteredBy() {
            return clusteredByValues;
        }

        public WhereClause toBoundWhereClause(DocTableInfo table,
                                              Row params,
                                              SubQueryResults subQueryResults,
                                              CoordinatorTxnCtx txnCtx,
                                              NodeContext nodeCtx) {
            SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
            Symbol boundQuery = binder.apply(query);
            HashSet<Symbol> clusteredBy = new HashSet<>(clusteredByValues.size());
            for (Symbol clusteredByValue : clusteredByValues) {
                clusteredBy.add(binder.apply(clusteredByValue));
            }
            if (table.isPartitioned()) {
                if (table.partitions().isEmpty()) {
                    return WhereClause.NO_MATCH;
                }
                WhereClauseAnalyzer.PartitionResult partitionResult =
                    WhereClauseAnalyzer.resolvePartitions(boundQuery, table, txnCtx, nodeCtx);
                return new WhereClause(
                    partitionResult.query,
                    partitionResult.partitions,
                    clusteredBy
                );
            } else {
                return new WhereClause(
                    boundQuery,
                    Collections.emptyList(),
                    clusteredBy
                );
            }
        }

        public boolean queryHasPkSymbolsOnly() {
            return queryHasPkSymbolsOnly;
        }
    }

    public static DetailedQuery optimize(EvaluatingNormalizer normalizer,
                                         Symbol query,
                                         DocTableInfo table,
                                         TransactionContext txnCtx,
                                         NodeContext nodeCtx) {
        Symbol queryGenColsProcessed = GeneratedColumnExpander.maybeExpand(
            query,
            table.generatedColumns(),
            Lists2.concat(table.partitionedByColumns(), Lists2.map(table.primaryKey(), table::getReference)),
            nodeCtx);
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, txnCtx);
        }
        WhereClause.validateVersioningColumnsUsage(query);

        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        boolean sequenceVersioningInQuery = Symbols.containsColumn(query, DocSysColumns.SEQ_NO) &&
                                            Symbols.containsColumn(query, DocSysColumns.PRIMARY_TERM);
        List<ColumnIdent> pkCols = pkColsInclVersioning(table, versionInQuery, sequenceVersioningInQuery);

        EqualityExtractor eqExtractor = new EqualityExtractor(normalizer);

        EqMatches pkMatches = eqExtractor.extractMatches(pkCols, query, txnCtx);
        Set<Symbol> clusteredBy = Collections.emptySet();
        if (table.clusteredBy() != null) {
            EqualityExtractor.EqMatches clusteredByMatches = eqExtractor.extractParentMatches(
                Collections.singletonList(table.clusteredBy()), query, txnCtx);
            List<List<Symbol>> clusteredBySymbols = clusteredByMatches.matches();
            if (clusteredBySymbols != null) {
                clusteredBy = new HashSet<>(clusteredBySymbols.size());
                for (List<Symbol> s : clusteredBySymbols) {
                    clusteredBy.add(s.get(0));
                }
            }
        }
        int clusterIdxWithinPK = table.primaryKey().indexOf(table.clusteredBy());
        final DocKeys docKeys;
        final boolean shouldUseDocKeys = table.isPartitioned() == false && (
                DocSysColumns.ID.equals(table.clusteredBy()) || (
                    table.primaryKey().size() == 1 && table.clusteredBy().equals(table.primaryKey().get(0))));

        if (pkMatches.matches() == null && shouldUseDocKeys) {
            pkMatches = eqExtractor.extractMatches(List.of(DocSysColumns.ID), query, txnCtx);
        }

        if (pkMatches.matches() == null) {
            docKeys = null;
        } else {
            List<Integer> partitionIndicesWithinPks = null;
            if (table.isPartitioned()) {
                partitionIndicesWithinPks = getPartitionIndices(table.primaryKey(), table.partitionedBy());
            }
            docKeys = new DocKeys(pkMatches.matches(),
                                  versionInQuery,
                                  sequenceVersioningInQuery,
                                  clusterIdxWithinPK,
                                  partitionIndicesWithinPks);
        }
        EqMatches partitionMatches = table.isPartitioned()
            ? eqExtractor.extractMatches(table.partitionedBy(), query, txnCtx)
            : EqMatches.NONE;

        WhereClauseValidator.validate(query);
        return new DetailedQuery(
            query,
            docKeys,
            partitionMatches.matches(),
            clusteredBy,
            table,
            pkMatches.unknowns().isEmpty()
        );
    }

    private static List<Integer> getPartitionIndices(List<ColumnIdent> pkCols, List<ColumnIdent> partitionCols) {
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

    private static List<ColumnIdent> pkColsInclVersioning(DocTableInfo table,
                                                          boolean versionInQuery,
                                                          boolean seqNoAndPrimaryTermInQuery) {
        if (versionInQuery) {
            ArrayList<ColumnIdent> pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
            return pkCols;
        } else if (seqNoAndPrimaryTermInQuery) {
            ArrayList<ColumnIdent> pkCols = new ArrayList<>(table.primaryKey().size() + 2);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.SEQ_NO);
            pkCols.add(DocSysColumns.PRIMARY_TERM);
            return pkCols;
        }
        return table.primaryKey();
    }
}
