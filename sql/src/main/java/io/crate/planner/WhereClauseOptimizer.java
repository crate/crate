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

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.EqualityExtractor;
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

public class WhereClauseOptimizer {

    public static class DetailedQuery {

        private final Symbol query;
        private final DocKeys docKeys;
        private final Set<Symbol> clusteredBy;
        private final List<List<Symbol>> partitionValues;

        public DetailedQuery(Symbol query, DocKeys docKeys, Set<Symbol> clusteredBy, List<List<Symbol>> partitionValues) {
            this.query = query;
            this.docKeys = docKeys;
            this.clusteredBy = clusteredBy;
            this.partitionValues = firstNonNull(partitionValues, Collections.emptyList());
        }

        public Optional<DocKeys> docKeys() {
            return Optional.ofNullable(docKeys);
        }

        public List<List<Symbol>> partitions() {
            return partitionValues;
        }
    }

    public static DetailedQuery optimize(EvaluatingNormalizer normalizer,
                                         Symbol query,
                                         DocTableInfo table,
                                         TransactionContext txnCtx) {
        query = GeneratedColumnExpander.maybeExpand(query, table.generatedColumns(), table.partitionedByColumns());

        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        List<ColumnIdent> pkCols = pkColsInclVersion(table, versionInQuery);

        EqualityExtractor eqExtractor = new EqualityExtractor(normalizer);
        List<List<Symbol>> pkValues = eqExtractor.extractExactMatches(pkCols, query, txnCtx);

        int clusterdIdxWithinPK = table.primaryKey().indexOf(table.clusteredBy());
        final Set<Symbol> clusteredBy;
        final DocKeys docKeys;
        if (pkValues == null) {
            docKeys = null;
            if (table.clusteredBy() == null) {
                clusteredBy = null;
            } else {
                clusteredBy = getClusteredBy(table.clusteredBy(), query, eqExtractor, txnCtx);
            }
        } else {
            List<Integer> partitionIndicesWithinPks = null;
            if (table.isPartitioned()) {
                partitionIndicesWithinPks = getPartitionIndices(table.primaryKey(), table.partitionedBy());
            }
            clusteredBy = clusteredByFromPKValues(pkValues, clusterdIdxWithinPK);
            docKeys = new DocKeys(pkValues, versionInQuery, clusterdIdxWithinPK, partitionIndicesWithinPks);
        }
        List<List<Symbol>> partitionValues = null;
        if (table.isPartitioned() && docKeys == null) {
            if (table.partitions().isEmpty()) {
                query = Literal.BOOLEAN_FALSE;
            } else {
                partitionValues = eqExtractor.extractExactMatches(table.partitionedBy(), query, txnCtx);
            }
        }
        return new DetailedQuery(query, docKeys, clusteredBy, partitionValues);
    }

    @Nullable
    private static Set<Symbol> clusteredByFromPKValues(List<List<Symbol>> pkValues, int clusterdIdxWithinPK) {
        if (clusterdIdxWithinPK >= 0) {
            HashSet<Symbol> clusteredBy = new HashSet<>();
            for (List<Symbol> pkValue : pkValues) {
                clusteredBy.add(pkValue.get(clusterdIdxWithinPK));
            }
            return clusteredBy;
        }
        return null;
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

    private static Set<Symbol> getClusteredBy(ColumnIdent clusteredByCol,
                                              Symbol query,
                                              EqualityExtractor eqExtractor,
                                              TransactionContext txnCtx) {
        List<List<Symbol>> clusteredByValues =
            eqExtractor.extractExactMatches(Collections.singletonList(clusteredByCol), query, txnCtx);
        if (clusteredByValues == null) {
            return null;
        }
        HashSet<Symbol> clusteredBy = new HashSet<>(clusteredByValues.size());
        for (List<Symbol> clusteredByValue : clusteredByValues) {
            assert clusteredByValue.size() == 1
                : "There must be only a single clusteredByValue because there is only a single clusteredBy column";
            clusteredBy.add(clusteredByValue.get(0));
        }
        return clusteredBy;
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
