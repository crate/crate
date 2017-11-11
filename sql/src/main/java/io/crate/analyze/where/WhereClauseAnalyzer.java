/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.where;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.SymbolToTrueVisitor;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.PartitionReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.planner.WhereClauseOptimizer;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WhereClauseAnalyzer {

    private final Functions functions;
    private final DocTableInfo table;
    private final EqualityExtractor eqExtractor;
    private final EvaluatingNormalizer normalizer;

    public WhereClauseAnalyzer(Functions functions, DocTableRelation tableRelation) {
        this.functions = functions;
        this.table = tableRelation.tableInfo();
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, tableRelation);
        this.eqExtractor = new EqualityExtractor(normalizer);
    }

    public WhereClause analyze(Symbol query, TransactionContext transactionContext) {
        if (query.equals(Literal.BOOLEAN_TRUE)) {
            return WhereClause.MATCH_ALL;
        }
        WhereClauseValidator.validate(query);
        Symbol queryGenColsProcessed = GeneratedColumnExpander.maybeExpand(
            query, table.generatedColumns(), table.partitionedByColumns());
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, transactionContext);
        }

        List<ColumnIdent> pkCols;
        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        if (versionInQuery) {
            pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
        } else {
            pkCols = table.primaryKey();
        }
        List<List<Symbol>> pkValues = eqExtractor.extractExactMatches(pkCols, query, transactionContext);
        Set<Symbol> clusteredBy = null;
        DocKeys docKeys = null;
        if (!pkCols.isEmpty() && pkValues != null) {
            int clusterdIdx = -1;
            if (table.clusteredBy() != null) {
                clusterdIdx = table.primaryKey().indexOf(table.clusteredBy());
                clusteredBy = new HashSet<>(pkValues.size());
                if (clusterdIdx >= 0) {
                    for (List<Symbol> row : pkValues) {
                        clusteredBy.add(row.get(clusterdIdx));
                    }
                }
            }
            List<Integer> partitionsIdx = null;
            if (table.isPartitioned()) {
                partitionsIdx = WhereClauseOptimizer.getPartitionIndices(table.primaryKey(), table.partitionedBy());
            }
            docKeys = new DocKeys(pkValues, versionInQuery, clusterdIdx, partitionsIdx);
        } else {
            clusteredBy = getClusteredByLiterals(query, eqExtractor, transactionContext);
        }

        List<String> partitions = null;
        if (table.isPartitioned() && docKeys == null) {
            if (table.partitions().isEmpty()) {
                return WhereClause.NO_MATCH;
            }
            PartitionResult partitionResult = resolvePartitions(query, table, functions, transactionContext);
            partitions = partitionResult.partitions;
            query = partitionResult.query;
        }
        return new WhereClause(query, docKeys, partitions, clusteredBy);
    }

    @Nullable
    private Set<Symbol> getClusteredByLiterals(Symbol query, EqualityExtractor ee, TransactionContext transactionContext) {
        if (table.clusteredBy() != null) {
            List<List<Symbol>> clusteredValues = ee.extractParentMatches(
                ImmutableList.of(table.clusteredBy()), query, transactionContext);
            if (clusteredValues != null) {
                Set<Symbol> clusteredBy = new HashSet<>(clusteredValues.size());
                for (List<Symbol> row : clusteredValues) {
                    clusteredBy.add(row.get(0));
                }
                return clusteredBy;
            }
        }
        return null;
    }


    private static PartitionReferenceResolver preparePartitionResolver(List<Reference> partitionColumns) {
        List<PartitionExpression> partitionExpressions = new ArrayList<>(partitionColumns.size());
        int idx = 0;
        for (Reference partitionedByColumn : partitionColumns) {
            partitionExpressions.add(new PartitionExpression(partitionedByColumn, idx));
            idx++;
        }
        return new PartitionReferenceResolver(partitionExpressions);
    }

    private static class PartitionResult {
        private final Symbol query;
        private final List<String> partitions;

        PartitionResult(Symbol query, List<String> partitions) {
            this.query = query;
            this.partitions = partitions;
        }
    }

    private static PartitionResult resolvePartitions(Symbol query,
                                                     DocTableInfo tableInfo,
                                                     Functions functions,
                                                     TransactionContext transactionContext) {
        assert tableInfo.isPartitioned() : "table must be partitioned in order to resolve partitions";
        assert !tableInfo.partitions().isEmpty() : "table must have at least one partition";

        PartitionReferenceResolver partitionReferenceResolver = preparePartitionResolver(
            tableInfo.partitionedByColumns());
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.PARTITION, partitionReferenceResolver, null);

        Symbol normalized;
        Map<Symbol, List<Literal>> queryPartitionMap = new HashMap<>();

        for (PartitionName partitionName : tableInfo.partitions()) {
            for (PartitionExpression partitionExpression : partitionReferenceResolver.expressions()) {
                partitionExpression.setNextRow(partitionName);
            }
            normalized = normalizer.normalize(query, transactionContext);
            assert normalized != null : "normalizing a query must not return null";

            if (normalized.equals(query)) {
                return new PartitionResult(query, Collections.emptyList()); // no partition columns inside the where clause
            }

            boolean canMatch = WhereClause.canMatch(normalized);
            if (canMatch) {
                List<Literal> partitions = queryPartitionMap.get(normalized);
                if (partitions == null) {
                    partitions = new ArrayList<>();
                    queryPartitionMap.put(normalized, partitions);
                }
                partitions.add(Literal.of(partitionName.asIndexName()));
            }
        }

        if (queryPartitionMap.size() == 1) {
            Map.Entry<Symbol, List<Literal>> entry = Iterables.getOnlyElement(queryPartitionMap.entrySet());
            return new PartitionResult(
                entry.getKey(), Lists2.copyAndReplace(entry.getValue(), ValueSymbolVisitor.STRING.function));
        } else if (queryPartitionMap.size() > 0) {
            return tieBreakPartitionQueries(normalizer, queryPartitionMap, transactionContext);
        } else {
            return new PartitionResult(Literal.BOOLEAN_FALSE, Collections.emptyList());
        }
    }

    private static PartitionResult tieBreakPartitionQueries(EvaluatingNormalizer normalizer,
                                                            Map<Symbol, List<Literal>> queryPartitionMap,
                                                            TransactionContext transactionContext) throws UnsupportedOperationException {
        /*
         * Got multiple normalized queries which all could match.
         * This might be the case if one partition resolved to null
         *
         * e.g.
         *
         *  p = 1 and x = 2
         *
         * might lead to
         *
         *  null and x = 2
         *  true and x = 2
         *
         * At this point it is unknown if they really match.
         * In order to figure out if they could potentially match all conditions involving references are now set to true
         *
         *  null and true   -> can't match
         *  true and true   -> can match, can use this query + partition
         *
         * If there is still more than 1 query that can match it's not possible to execute the query :(
         */

        List<Tuple<Symbol, List<Literal>>> canMatch = new ArrayList<>();
        SymbolToTrueVisitor symbolToTrueVisitor = new SymbolToTrueVisitor();
        for (Map.Entry<Symbol, List<Literal>> entry : queryPartitionMap.entrySet()) {
            Symbol query = entry.getKey();
            List<Literal> partitions = entry.getValue();

            Symbol symbol = symbolToTrueVisitor.process(query, null);
            Symbol normalized = normalizer.normalize(symbol, transactionContext);

            assert normalized instanceof Literal :
                "after normalization and replacing all reference occurrences with true there must only be a literal left";

            Object value = ((Literal) normalized).value();
            if (value != null && (Boolean) value) {
                canMatch.add(new Tuple<>(query, partitions));
            }
        }
        if (canMatch.size() == 1) {
            Tuple<Symbol, List<Literal>> symbolListTuple = canMatch.get(0);
            return new PartitionResult(
                symbolListTuple.v1(),
                Lists2.copyAndReplace(symbolListTuple.v2(), ValueSymbolVisitor.STRING.function));
        }
        throw new UnsupportedOperationException(
            "logical conjunction of the conditions in the WHERE clause which " +
            "involve partitioned columns led to a query that can't be executed.");
    }
}
