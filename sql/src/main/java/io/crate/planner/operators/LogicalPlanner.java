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

package io.crate.planner.operators;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Planner which can create a {@link Plan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    public Plan plan(QueriedRelation queriedRelation,
                     Planner.Context plannerContext,
                     ProjectionBuilder projectionBuilder,
                     FetchMode fetchMode) {
        LogicalPlan logicalPlan = plan(queriedRelation, fetchMode, true)
            .build(new HashSet<>(queriedRelation.outputs()))
            .tryCollapse();

        Plan plan = logicalPlan.build(
            plannerContext,
            projectionBuilder,
            LogicalPlanner.NO_LIMIT,
            0,
            null,
            null
        );
        return plan;
    }

    static LogicalPlan.Builder plan(QueriedRelation relation, FetchMode fetchMode, boolean isLastFetch) {
        SplitPoints splitPoints = SplitPoints.create(relation.querySpec());
        LogicalPlan.Builder sourceBuilder =
            FetchOrEval.create(
                Limit.create(
                    Order.create(
                        Filter.create(
                            groupByOrAggregate(
                                collectAndFilter(
                                    relation,
                                    splitPoints.toCollect(),
                                    relation.where()
                                ),
                                relation.groupBy(),
                                splitPoints.aggregates()),
                            relation.having()
                        ),
                        relation.orderBy()
                    ),
                    relation.limit(),
                    relation.offset()
                ),
                relation.querySpec().outputs(),
                fetchMode,
                isLastFetch
            );
        if (isLastFetch) {
            return sourceBuilder;
        }
        return RelationBoundary.create(sourceBuilder, relation);
    }

    private static LogicalPlan.Builder groupByOrAggregate(LogicalPlan.Builder source,
                                                          List<Symbol> groupKeys,
                                                          List<Function> aggregates) {
        if (!groupKeys.isEmpty()) {
            return GroupHashAggregate.create(source, groupKeys, aggregates);
        }
        if (!aggregates.isEmpty()) {
            return usedColumns -> new HashAggregate(source.build(extractColumns(aggregates)), aggregates);
        }
        return source;
    }

    private static LogicalPlan.Builder collectAndFilter(QueriedRelation queriedRelation,
                                                        List<Symbol> toCollect,
                                                        WhereClause where) {
        if (queriedRelation instanceof QueriedTableRelation) {
            return createCollect((QueriedTableRelation) queriedRelation, toCollect, where);
        }
        if (queriedRelation instanceof MultiSourceSelect) {
            return Join.createNodes(((MultiSourceSelect) queriedRelation), where);
        }
        if (queriedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation selectRelation = (QueriedSelectRelation) queriedRelation;
            return Filter.create(
                plan(selectRelation.subRelation(), FetchMode.WITH_PROPAGATION, false),
                where
            );
        }
        throw new UnsupportedOperationException("Cannot create LogicalPlan from: " + queriedRelation);
    }

    private static LogicalPlan.Builder createCollect(QueriedTableRelation relation,
                                                     List<Symbol> toCollect,
                                                     WhereClause where) {
        return usedColumns -> new Collect(relation, toCollect, where, usedColumns);
    }

    static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }
}
