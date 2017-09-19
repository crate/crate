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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.TableFunctionCollectPhase;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class Collect implements LogicalPlan {

    private static final String COLLECT_PHASE_NAME = "collect";
    final QueriedTableRelation relation;
    final WhereClause where;

    final List<Symbol> toCollect;
    final TableInfo tableInfo;

    Collect(QueriedTableRelation relation, List<Symbol> toCollect, WhereClause where, Set<Symbol> usedColumns) {
        this.relation = relation;
        this.where = where;
        this.tableInfo = relation.tableRelation().tableInfo();
        if (where.hasQuery() && !(tableInfo instanceof DocTableInfo)) {
            NoPredicateVisitor.ensureNoMatchPredicate(where.query());
        }
        this.toCollect = toCollect;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order) {
        RoutedCollectPhase collectPhase = createPhase(plannerContext);
        collectPhase.orderBy(order);
        return new io.crate.planner.node.dql.Collect(
            collectPhase,
            limit,
            offset,
            toCollect.size(),
            limit,
            PositionalOrderBy.of(order, toCollect)
        );
    }

    private RoutedCollectPhase createPhase(Planner.Context plannerContext) {
        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
        if (relation.tableRelation() instanceof TableFunctionRelation) {
            TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) relation.tableRelation();
            List<Symbol> args = tableFunctionRelation.function().arguments();
            ArrayList<Literal<?>> functionArguments = new ArrayList<>(args.size());
            for (Symbol arg : args) {
                // It's not possible to use columns as argument to a table function and subqueries are currently not allowed either.
                functionArguments.add((Literal) plannerContext.normalizer().normalize(arg, plannerContext.transactionContext()));
            }
            return new TableFunctionCollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                plannerContext.allocateRouting(tableInfo, where, null, sessionContext),
                tableFunctionRelation.functionImplementation(),
                functionArguments,
                Collections.emptyList(),
                toCollect,
                where
            );
        }
        return new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COLLECT_PHASE_NAME,
            plannerContext.allocateRouting(
                tableInfo,
                where,
                null,
                sessionContext),
            tableInfo.rowGranularity(),
            toCollect,
            Collections.emptyList(),
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            sessionContext.user()
        );
    }

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public RowGranularity dataGranularity() {
        return tableInfo.rowGranularity();
    }


    private static final  class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        private NoPredicateVisitor() {
        }

        static void ensureNoMatchPredicate(Symbol symbolTree) {
            NO_PREDICATE_VISITOR.process(symbolTree, null);
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
            if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return null;
        }
    }
}
