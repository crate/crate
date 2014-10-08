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

import io.crate.PartitionName;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.ReferenceToTrueVisitor;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionReferenceResolver;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Analyzes a WhereClause for partition columns and returns a new WhereClause
 * which has its {@link io.crate.analyze.where.WhereClause#partitions()} set to all partitions that can match.
 */
public class PartitionResolver {

    private final ReferenceResolver referenceResolver;
    private final Functions functions;

    public PartitionResolver(ReferenceResolver referenceResolver, Functions functions) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
    }

    private PartitionReferenceResolver preparePartitionResolver(List<ReferenceInfo> partitionColumns) {
        List<PartitionExpression> partitionExpressions = new ArrayList<>(partitionColumns.size());
        int idx = 0;
        for (ReferenceInfo partitionedByColumn : partitionColumns) {
            partitionExpressions.add(new PartitionExpression(partitionedByColumn, idx));
            idx++;
        }
        return new PartitionReferenceResolver(referenceResolver, partitionExpressions);
    }

    public WhereClause resolvePartitions(WhereClause whereClause,
                                         TableInfo table) {
        assert table.isPartitioned() : "table must be partitioned in order to resolve partitions";
        if (table.partitions().isEmpty()) {
            return WhereClause.NO_MATCH; // table is partitioned but has no data / no partitions
        }
        PartitionReferenceResolver partitionReferenceResolver = preparePartitionResolver(
                table.partitionedByColumns());
        EvaluatingNormalizer normalizer =
                new EvaluatingNormalizer(functions, RowGranularity.PARTITION, partitionReferenceResolver);

        Symbol normalized;
        Map<Symbol, List<Literal>> queryPartitionMap = new HashMap<>();

        for (PartitionName partitionName : table.partitions()) {
            for (PartitionExpression partitionExpression : partitionReferenceResolver.expressions()) {
                partitionExpression.setNextRow(partitionName);
            }
            normalized = normalizer.normalize(whereClause.query());
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
                partitions.add(Literal.newLiteral(partitionName.stringValue()));
            }
        }

        if (queryPartitionMap.size() == 1) {
            Map.Entry<Symbol, List<Literal>> entry = queryPartitionMap.entrySet().iterator().next();
            whereClause = new WhereClause(entry.getKey());
            whereClause.partitions(entry.getValue());
        } else if (queryPartitionMap.size() > 0) {
            whereClause = tieBreakPartitionQueries(normalizer, queryPartitionMap);
        } else {
            whereClause = WhereClause.NO_MATCH;
        }

        return whereClause;
    }

    private WhereClause tieBreakPartitionQueries(EvaluatingNormalizer normalizer,
                                                 Map<Symbol, List<Literal>> queryPartitionMap)
            throws UnsupportedOperationException{
        /**
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
            Symbol normalized = normalizer.normalize(symbol);

            assert normalized instanceof Literal && ((Literal) normalized).valueType().equals(DataTypes.BOOLEAN) :
                    "after normalization and replacing all reference occurrences with true there must only be a boolean left";

            Object value = ((Literal) normalized).value();
            if (value != null && (Boolean) value) {
                canMatch.add(new Tuple<>(query, partitions));
            }
        }
        if (canMatch.size() == 1) {
            Tuple<Symbol, List<Literal>> symbolListTuple = canMatch.get(0);
            WhereClause whereClause = new WhereClause(symbolListTuple.v1());
            whereClause.partitions(symbolListTuple.v2());
            return whereClause;
        }
        throw new UnsupportedOperationException(
                "logical conjunction of the conditions in the WHERE clause which " +
                        "involve partitioned columns led to a query that can't be executed.");
    }
}
