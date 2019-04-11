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

import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.WhereClause;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.EqualityExtractor;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

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
        private final List<List<Symbol>> partitions;
        private final Set<Symbol> clusteredByValues;

        DetailedQuery(Symbol query,
                      DocKeys docKeys,
                      List<List<Symbol>> partitionValues,
                      Set<Symbol> clusteredByValues) {
            this.query = query;
            this.docKeys = docKeys;
            this.partitions = firstNonNull(partitionValues, Collections.emptyList());
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
            return partitions;
        }

        public Symbol query() {
            return query;
        }

        public Set<Symbol> clusteredBy() {
            return clusteredByValues;
        }

        public WhereClause toBoundWhereClause(DocTableInfo table,
                                              Functions functions,
                                              Row params,
                                              SubQueryResults subQueryResults,
                                              CoordinatorTxnCtx txnCtx) {
            if (docKeys != null) {
                throw new IllegalStateException(getClass().getSimpleName()
                                                + " must not be converted to a WhereClause if docKeys are present");
            }
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
                    WhereClauseAnalyzer.resolvePartitions(boundQuery, table, functions, txnCtx);
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
    }

    public static DetailedQuery optimize(EvaluatingNormalizer normalizer,
                                         Symbol query,
                                         DocTableInfo table,
                                         CoordinatorTxnCtx txnCtx) {
        Symbol queryGenColsProcessed = GeneratedColumnExpander.maybeExpand(
            query,
            table.generatedColumns(),
            Lists2.concat(table.partitionedByColumns(), Lists2.map(table.primaryKey(), table::getReference)));
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, txnCtx);
        }

        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        boolean sequenceVersioningInQuery = Symbols.containsColumn(query, DocSysColumns.SEQ_NO) &&
                                            Symbols.containsColumn(query, DocSysColumns.PRIMARY_TERM);
        List<ColumnIdent> pkCols = pkColsInclVersioning(table, versionInQuery, sequenceVersioningInQuery);

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
            docKeys = new DocKeys(pkValues,
                                  versionInQuery,
                                  sequenceVersioningInQuery,
                                  clusterdIdxWithinPK,
                                  partitionIndicesWithinPks);
        }
        List<List<Symbol>> partitionValues = null;
        if (table.isPartitioned()) {
            partitionValues = eqExtractor.extractExactMatches(table.partitionedBy(), query, txnCtx);
        }
        Set<Symbol> clusteredBy = Collections.emptySet();
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
        if (docKeys == null) {
            WhereClauseValidator.validate(query);
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

    private static List<ColumnIdent> pkColsInclVersioning(DocTableInfo table,
                                                          boolean versionInQuery,
                                                          boolean seqNoAndPrimaryTermInQuery) {
        if (versionInQuery) {
            ArrayList<ColumnIdent> pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
            return pkCols;
        } else if (seqNoAndPrimaryTermInQuery) {
            ArrayList<ColumnIdent> pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.SEQ_NO);
            pkCols.add(DocSysColumns.PRIMARY_TERM);
            return pkCols;
        }
        return table.primaryKey();
    }
}
