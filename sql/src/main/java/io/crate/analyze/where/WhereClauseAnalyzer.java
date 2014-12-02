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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.crate.PartitionName;
import io.crate.analyze.*;
import io.crate.metadata.PartitionReferenceResolver;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.BytesRefValueSymbolVisitor;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.*;

public class WhereClauseAnalyzer {

    private final static PrimaryKeyVisitor PRIMARY_KEY_VISITOR = new PrimaryKeyVisitor();
    private final AnalysisMetaData analysisMetaData;
    private final TableInfo tableInfo;

    public WhereClauseAnalyzer(AnalysisMetaData analysisMetaData, TableInfo tableInfo) {
        this.analysisMetaData = analysisMetaData;
        this.tableInfo = tableInfo;
    }

    public WhereClauseContext analyze(WhereClause whereClause) {
        if (whereClause.noMatch() || !whereClause.hasQuery()) {
            return new WhereClauseContext(whereClause);
        }
        WhereClauseContext whereClauseContext = new WhereClauseContext(whereClause);
        PrimaryKeyVisitor.Context ctx = PRIMARY_KEY_VISITOR.process(tableInfo, Field.unwrap(whereClause.query()));
        if (ctx != null) {
            whereClause.clusteredByLiteral(ctx.clusteredByLiteral());
            if (ctx.noMatch) {
                whereClauseContext.whereClause(WhereClause.NO_MATCH);
            } else {
                whereClause.version(ctx.version());

                if (ctx.keyLiterals() != null) {
                    processPrimaryKeyLiterals(
                            ctx.keyLiterals(),
                            whereClause.clusteredBy().orNull(),
                            whereClauseContext
                    );
                }
            }
        }
        if (tableInfo.isPartitioned()) {
            resolvePartitions(whereClauseContext);
        }
        return whereClauseContext;
    }


    protected void processPrimaryKeyLiterals(List primaryKeyLiterals,
                                                    @Nullable String clusteredBy,
                                                    WhereClauseContext context) {
        List<List<BytesRef>> primaryKeyValuesList = new ArrayList<>(primaryKeyLiterals.size());
        primaryKeyValuesList.add(new ArrayList<BytesRef>(tableInfo.primaryKey().size()));

        for (int i=0; i<primaryKeyLiterals.size(); i++) {
            Object primaryKey = primaryKeyLiterals.get(i);
            if (primaryKey instanceof Literal) {
                Literal pkLiteral = (Literal)primaryKey;
                // support e.g. pk IN (Value..,)
                if (pkLiteral.valueType().id() == SetType.ID) {
                    Set<Literal> literals = Sets.newHashSet(Literal.explodeCollection(pkLiteral));
                    Iterator<Literal> literalIterator = literals.iterator();
                    for (int s=0; s<literals.size(); s++) {
                        Literal pk = literalIterator.next();
                        if (s >= primaryKeyValuesList.size()) {
                            // copy already parsed pk values, so we have all possible multiple pk for all sets
                            primaryKeyValuesList.add(Lists.newArrayList(primaryKeyValuesList.get(s - 1)));
                        }
                        List<BytesRef> primaryKeyValues = primaryKeyValuesList.get(s);
                        if (primaryKeyValues.size() > i) {
                            primaryKeyValues.set(i, BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        } else {
                            primaryKeyValues.add(BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        }
                    }
                } else {
                    for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
                        primaryKeyValues.add((BytesRefValueSymbolVisitor.INSTANCE.process(pkLiteral)));
                    }
                }
            } else if (primaryKey instanceof List) {
                primaryKey = Lists.transform((List<Literal>) primaryKey, new com.google.common.base.Function<Literal, BytesRef>() {
                    @Override
                    public BytesRef apply(Literal input) {
                        return BytesRefValueSymbolVisitor.INSTANCE.process(input);
                    }
                });
                primaryKeyValuesList.add((List<BytesRef>) primaryKey);
            }
        }

        for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
            context.addIdAndRouting(tableInfo, primaryKeyValues, clusteredBy);
        }
    }

    private PartitionReferenceResolver preparePartitionResolver(
            ReferenceResolver referenceResolver, List<ReferenceInfo> partitionColumns) {
        List<PartitionExpression> partitionExpressions = new ArrayList<>(partitionColumns.size());
        int idx = 0;
        for (ReferenceInfo partitionedByColumn : partitionColumns) {
            partitionExpressions.add(new PartitionExpression(partitionedByColumn, idx));
            idx++;
        }
        return new PartitionReferenceResolver(referenceResolver, partitionExpressions);
    }

    private void resolvePartitions(WhereClauseContext whereClauseContext) {
        assert tableInfo.isPartitioned() : "table must be partitioned in order to resolve partitions";
        if (tableInfo.partitions().isEmpty()) {
            whereClauseContext.whereClause(WhereClause.NO_MATCH); // table is partitioned but has no data / no partitions
        }

        WhereClause whereClause = whereClauseContext.whereClause();
        PartitionReferenceResolver partitionReferenceResolver = preparePartitionResolver(
                analysisMetaData.referenceResolver(),
                tableInfo.partitionedByColumns());
        EvaluatingNormalizer normalizer =
                new EvaluatingNormalizer(analysisMetaData.functions(), RowGranularity.PARTITION, partitionReferenceResolver);

        Symbol normalized = null;
        Map<Symbol, List<Literal>> queryPartitionMap = new HashMap<>();

        for (PartitionName partitionName : tableInfo.partitions()) {
            for (PartitionExpression partitionExpression : partitionReferenceResolver.expressions()) {
                partitionExpression.setNextRow(partitionName);
            }
            normalized = normalizer.normalize(whereClause.query());
            assert normalized != null : "normalizing a query must not return null";

            if (normalized.equals(whereClause.query())) {
                return; // no partition columns inside the where clause
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
            whereClauseContext.whereClause(whereClause);
        } else if (queryPartitionMap.size() > 0) {
            whereClauseContext.whereClause(tieBreakPartitionQueries(normalizer, queryPartitionMap));
        } else {
            whereClauseContext.whereClause(WhereClause.NO_MATCH);
        }
    }

    private WhereClause tieBreakPartitionQueries(EvaluatingNormalizer normalizer,
                                                 Map<Symbol, List<Literal>> queryPartitionMap) throws UnsupportedOperationException{
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

            assert normalized instanceof Literal && normalized.valueType().equals(DataTypes.BOOLEAN) :
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
