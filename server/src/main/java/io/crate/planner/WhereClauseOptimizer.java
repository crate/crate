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

import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.WhereClause;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.EqualityExtractor;
import io.crate.analyze.where.EqualityExtractor.EqMatches;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.symbol.Optimizer;

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
                                              NodeContext nodeCtx,
                                              Metadata metadata) {
            SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
            Symbol boundQuery = binder.apply(query);
            HashSet<Symbol> clusteredBy = HashSet.newHashSet(clusteredByValues.size());
            for (Symbol clusteredByValue : clusteredByValues) {
                clusteredBy.add(binder.apply(clusteredByValue));
            }
            if (table.isPartitioned()) {
                if (table.getPartitionNames(metadata).isEmpty()) {
                    return WhereClause.NO_MATCH;
                }
                WhereClauseAnalyzer.PartitionResult partitionResult =
                    WhereClauseAnalyzer.resolvePartitions(boundQuery, table, txnCtx, nodeCtx, metadata);
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
            Lists.concat(table.partitionedByColumns(), Lists.map(table.primaryKey(), table::getReference)),
            nodeCtx);
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, txnCtx);
        }
        WhereClause.validateVersioningColumnsUsage(query);

        boolean versionInQuery = query.hasColumn(DocSysColumns.VERSION);
        boolean sequenceVersioningInQuery = query.hasColumn(DocSysColumns.SEQ_NO) &&
                                            query.hasColumn(DocSysColumns.PRIMARY_TERM);
        List<ColumnIdent> pkCols = pkColsInclVersioning(table, versionInQuery, sequenceVersioningInQuery);

        EqualityExtractor eqExtractor = new EqualityExtractor(normalizer);

        // ExpressionAnalyzer will "blindly" add cast to the column, to match the datatype of the literal.
        // To be able to properly extract pkMatches (and therefore dockeys), so that the cast is moved to the literal,
        // we need to optimize the casts, before the extraction of pkMatches (and the dockeys after that)
        var optimizedCastsQuery = Optimizer.optimizeCasts(query, txnCtx, nodeCtx);
        EqMatches pkMatches = eqExtractor.extractMatches(pkCols, optimizedCastsQuery, txnCtx);
        Set<Symbol> clusteredBy = Collections.emptySet();
        if (table.clusteredBy() != null) {
            EqualityExtractor.EqMatches clusteredByMatches = eqExtractor.extractParentMatches(
                Collections.singletonList(table.clusteredBy()), optimizedCastsQuery, txnCtx);
            List<List<Symbol>> clusteredBySymbols = clusteredByMatches.matches();
            if (clusteredBySymbols != null) {
                clusteredBy = HashSet.newHashSet(clusteredBySymbols.size());
                for (List<Symbol> s : clusteredBySymbols) {
                    clusteredBy.add(s.getFirst());
                }
            }
        }
        int clusterIdxWithinPK = table.primaryKey().indexOf(table.clusteredBy());
        final DocKeys docKeys;
        final boolean shouldUseDocKeys = table.isPartitioned() == false && (
                DocSysColumns.ID.COLUMN.equals(table.clusteredBy()) || (
                    table.primaryKey().size() == 1 && table.clusteredBy().equals(table.primaryKey().getFirst())));

        if (pkMatches.matches() == null && shouldUseDocKeys) {
            pkMatches = eqExtractor.extractMatches(List.of(DocSysColumns.ID.COLUMN), optimizedCastsQuery, txnCtx);
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
            ? eqExtractor.extractMatches(table.partitionedBy(), optimizedCastsQuery, txnCtx)
            : EqMatches.NONE;

        WhereClauseValidator.validate(query);
        return new DetailedQuery(
            query,
            docKeys,
            partitionMatches.matches(),
            clusteredBy,
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
