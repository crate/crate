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
import io.crate.analyze.GeneratedColumnComparisonReplacer;
import io.crate.analyze.ReferenceToTrueVisitor;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.*;

public class WhereClauseAnalyzer {

    private final static GeneratedColumnComparisonReplacer GENERATED_COLUMN_COMPARISON_REPLACER = new GeneratedColumnComparisonReplacer();

    private final Functions functions;
    private final DocTableInfo tableInfo;
    private final EqualityExtractor eqExtractor;
    private final EvaluatingNormalizer normalizer;

    public WhereClauseAnalyzer(Functions functions, DocTableRelation tableRelation) {
        this.functions = functions;
        this.tableInfo = tableRelation.tableInfo();
        this.normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.CLUSTER, ReplaceMode.COPY, null, tableRelation);
        this.eqExtractor = new EqualityExtractor(normalizer);
    }

    public WhereClause analyze(WhereClause whereClause, TransactionContext transactionContext) {
        if (!whereClause.hasQuery()) {
            return whereClause;
        }
        Set<Symbol> clusteredBy = null;
        if (whereClause.hasQuery()) {
            WhereClauseValidator.validate(whereClause);
            Symbol query = GENERATED_COLUMN_COMPARISON_REPLACER.replaceIfPossible(whereClause.query(), tableInfo);
            if (!whereClause.query().equals(query)) {
                whereClause = new WhereClause(normalizer.normalize(query, transactionContext));
            }
        }

        List<ColumnIdent> pkCols;
        boolean versionInQuery = Symbols.containsColumn(whereClause.query(), DocSysColumns.VERSION);
        if (versionInQuery) {
            pkCols = new ArrayList<>(tableInfo.primaryKey().size() + 1);
            pkCols.addAll(tableInfo.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
        } else {
            pkCols = tableInfo.primaryKey();
        }
        List<List<Symbol>> pkValues = eqExtractor.extractExactMatches(pkCols, whereClause.query(), transactionContext);

        if (!pkCols.isEmpty() && pkValues != null) {
            int clusterdIdx = -1;
            if (tableInfo.clusteredBy() != null) {
                clusterdIdx = tableInfo.primaryKey().indexOf(tableInfo.clusteredBy());
                clusteredBy = new HashSet<>(pkValues.size());
            }
            List<Integer> partitionsIdx = null;
            if (tableInfo.isPartitioned()) {
                partitionsIdx = new ArrayList<>(tableInfo.partitionedByColumns().size());
                for (ColumnIdent columnIdent : tableInfo.partitionedBy()) {
                    int posPartitionColumn = tableInfo.primaryKey().indexOf(columnIdent);
                    if (posPartitionColumn >= 0) {
                        partitionsIdx.add(posPartitionColumn);
                    }
                }
            }
            whereClause.docKeys(new DocKeys(pkValues, versionInQuery, clusterdIdx, partitionsIdx));
            if (clusterdIdx >= 0) {
                for (List<Symbol> row : pkValues) {
                    clusteredBy.add(row.get(clusterdIdx));
                }
                whereClause.clusteredBy(clusteredBy);
            }
        } else {
            clusteredBy = getClusteredByLiterals(whereClause, eqExtractor, transactionContext);
        }
        if (clusteredBy != null) {
            whereClause.clusteredBy(clusteredBy);
        }
        if (tableInfo.isPartitioned() && !whereClause.docKeys().isPresent()) {
            whereClause = resolvePartitions(whereClause, tableInfo, functions, transactionContext);
        }
        return whereClause;
    }

    @Nullable
    private Set<Symbol> getClusteredByLiterals(WhereClause whereClause, EqualityExtractor ee, TransactionContext transactionContext) {
        if (tableInfo.clusteredBy() != null) {
            List<List<Symbol>> clusteredValues = ee.extractParentMatches(
                ImmutableList.of(tableInfo.clusteredBy()),
                whereClause.query(), transactionContext);
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

    private static WhereClause resolvePartitions(WhereClause whereClause,
                                                 DocTableInfo tableInfo,
                                                 Functions functions,
                                                 TransactionContext transactionContext) {
        assert tableInfo.isPartitioned() : "table must be partitioned in order to resolve partitions";
        assert whereClause.partitions().isEmpty() : "partitions must not be analyzed twice";
        if (tableInfo.partitions().isEmpty()) {
            return WhereClause.NO_MATCH; // table is partitioned but has no data / no partitions
        }

        PartitionReferenceResolver partitionReferenceResolver = preparePartitionResolver(
            tableInfo.partitionedByColumns());
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.PARTITION, ReplaceMode.COPY, partitionReferenceResolver, null);

        Symbol normalized;
        Map<Symbol, List<Literal>> queryPartitionMap = new HashMap<>();

        for (PartitionName partitionName : tableInfo.partitions()) {
            for (PartitionExpression partitionExpression : partitionReferenceResolver.expressions()) {
                partitionExpression.setNextRow(partitionName);
            }
            normalized = normalizer.normalize(whereClause.query(), transactionContext);
            assert normalized != null : "normalizing a query must not return null";

            if (normalized.equals(whereClause.query())) {
                return whereClause; // no partition columns inside the where clause
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
            whereClause = new WhereClause(
                entry.getKey(),
                whereClause.docKeys().orNull(),
                new ArrayList<String>(entry.getValue().size()));
            whereClause.partitions(entry.getValue());
            return whereClause;
        } else if (queryPartitionMap.size() > 0) {
            return tieBreakPartitionQueries(normalizer, queryPartitionMap, whereClause, transactionContext);
        } else {
            return WhereClause.NO_MATCH;
        }
    }

    private static WhereClause tieBreakPartitionQueries(EvaluatingNormalizer normalizer,
                                                        Map<Symbol, List<Literal>> queryPartitionMap,
                                                        WhereClause whereClause,
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
        ReferenceToTrueVisitor referenceToTrueVisitor = new ReferenceToTrueVisitor();
        for (Map.Entry<Symbol, List<Literal>> entry : queryPartitionMap.entrySet()) {
            Symbol query = entry.getKey();
            List<Literal> partitions = entry.getValue();

            Symbol symbol = referenceToTrueVisitor.process(query, null);
            Symbol normalized = normalizer.normalize(symbol, transactionContext);

            assert normalized instanceof Literal && normalized.valueType().equals(DataTypes.BOOLEAN) :
                "after normalization and replacing all reference occurrences with true there must only be a boolean left";

            Object value = ((Literal) normalized).value();
            if (value != null && (Boolean) value) {
                canMatch.add(new Tuple<>(query, partitions));
            }
        }
        if (canMatch.size() == 1) {
            Tuple<Symbol, List<Literal>> symbolListTuple = canMatch.get(0);
            WhereClause where = new WhereClause(symbolListTuple.v1(),
                whereClause.docKeys().orNull(),
                new ArrayList<String>(symbolListTuple.v2().size()));
            where.partitions(symbolListTuple.v2());
            return where;
        }
        throw new UnsupportedOperationException(
            "logical conjunction of the conditions in the WHERE clause which " +
            "involve partitioned columns led to a query that can't be executed.");
    }
}
