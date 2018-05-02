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

import com.google.common.collect.Iterables;
import io.crate.analyze.SymbolToTrueVisitor;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.partitioned.PartitionExpression;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.ValueSymbolVisitor;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.PartitionReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WhereClauseAnalyzer {

    /**
     * Replace parameters and sub-queries with the related values and analyze the query afterwards.
     */
    public static WhereClause bindAndAnalyze(WhereClause where,
                                             Row params,
                                             Map<SelectSymbol, Object> subQueryValues,
                                             AbstractTableRelation tableRelation,
                                             Functions functions,
                                             TransactionContext transactionContext) {
        if (where.hasQuery()) {
            Function<Symbol, Symbol> bind = s -> SubQueryAndParamBinder.convert(s, params, subQueryValues);
            Symbol query = bind.apply(where.query());
            Set<Symbol> clusteredBy = where.clusteredBy().stream().map(bind).collect(Collectors.toSet());
            if (tableRelation instanceof DocTableRelation) {
                DocTableInfo table = ((DocTableRelation) tableRelation).tableInfo();
                if (table.isPartitioned()) {
                    if (table.partitions().isEmpty()) {
                        return WhereClause.NO_MATCH;
                    }
                    PartitionResult partitionResult = resolvePartitions(query, table, functions, transactionContext);
                    if (!where.partitions().isEmpty()
                        && !partitionResult.partitions.isEmpty()
                        && !partitionResult.partitions.equals(where.partitions())) {

                        throw new IllegalArgumentException("Given partition ident does not match partition evaluated from where clause");
                    }
                    return new WhereClause(
                        partitionResult.query,
                        partitionResult.partitions,
                        clusteredBy
                    );
                }
                return new WhereClause(query, where.partitions(), clusteredBy);
            } else {
                return new WhereClause(query, where.partitions(), clusteredBy);
            }
        }
        return where;
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

    public static class PartitionResult {
        public final Symbol query;
        public final List<String> partitions;

        PartitionResult(Symbol query, List<String> partitions) {
            this.query = query;
            this.partitions = partitions;
        }
    }

    public static PartitionResult resolvePartitions(Symbol query,
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
