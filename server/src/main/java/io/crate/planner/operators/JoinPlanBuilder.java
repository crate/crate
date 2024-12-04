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

package io.crate.planner.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.sql.tree.JoinType;

/**
 * A logical plan builder for `Join` operations to build implicit joins from a given query.
 */
public class JoinPlanBuilder {

    static LogicalPlan buildJoinTree(List<AnalyzedRelation> from,
                                     Symbol whereClause,
                                     SubQueries subQueries,
                                     Function<AnalyzedRelation, LogicalPlan> toLogicalPlan) {
        if (from.size() == 1) {
            // We have only one relation, this means we have no implicit joins
            LogicalPlan logicalPlan = toLogicalPlan.apply(from.getFirst());
            LogicalPlan correlatedJoin = subQueries.applyCorrelatedJoin(logicalPlan);
            return Filter.create(correlatedJoin, whereClause);
        } else {
            // We have more than one relation, this means we have implicit joins in the query
            // Therefore, convert all relations from the from-clause to logical plans and join them
            Iterator<AnalyzedRelation> relations = from.iterator();

            LogicalPlan joinPlan = new JoinPlan(
                toLogicalPlan.apply(relations.next()),
                toLogicalPlan.apply(relations.next()),
                JoinType.CROSS,
                null);

            while (relations.hasNext()) {
                joinPlan = new JoinPlan(joinPlan, toLogicalPlan.apply(relations.next()), JoinType.CROSS, null);
            }

            CorrelatedSubQueries correlatedSubQueries = extractCorrelatedSubQueries(whereClause);

            if (correlatedSubQueries.correlatedSubQueries.isEmpty() == false) {
                joinPlan = subQueries.applyCorrelatedJoin(joinPlan);
            }

            if (subQueries.correlated().isEmpty() == false) {
                joinPlan = subQueries.applyCorrelatedJoin(joinPlan);
            }

            return Filter.create(joinPlan, whereClause);
        }
    }

    /**
     * Splits the given Symbol tree into a list of correlated sub-queries and a list of remaining symbols.
     */
    public static CorrelatedSubQueries extractCorrelatedSubQueries(@Nullable Symbol from) {
        if (from == null) {
            return new CorrelatedSubQueries(List.of(), List.of());
        }
        var values = QuerySplitter.split(from).values();
        var remainder = new ArrayList<Symbol>(values.size());
        var correlatedSubQueries = new ArrayList<Symbol>(values.size());
        for (var symbol : values) {
            if (symbol.any(s -> s instanceof SelectSymbol x && x.isCorrelated())) {
                correlatedSubQueries.add(symbol);
            } else {
                remainder.add(symbol);
            }
        }
        return new CorrelatedSubQueries(correlatedSubQueries, remainder);
    }

    public record CorrelatedSubQueries(List<Symbol> correlatedSubQueries, List<Symbol> remainder) {
    }
}
