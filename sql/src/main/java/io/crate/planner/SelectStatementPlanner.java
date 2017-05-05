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

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.ESGetStatementPlanner;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Map;

class SelectStatementPlanner {

    private final Visitor visitor;

    SelectStatementPlanner(ConsumingPlanner consumingPlanner, ClusterService clusterService) {
        visitor = new Visitor(consumingPlanner, clusterService);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<Planner.Context, Plan> {

        private final ConsumingPlanner consumingPlanner;
        private final ClusterService clusterService;

        public Visitor(ConsumingPlanner consumingPlanner, ClusterService clusterService) {
            this.consumingPlanner = consumingPlanner;
            this.clusterService = clusterService;
        }

        private Plan invokeConsumingPlanner(AnalyzedRelation relation, Planner.Context context) {
            Plan plan = consumingPlanner.plan(relation, context);
            if (plan == null) {
                throw new UnsupportedOperationException("Cannot create plan for: " + relation);
            }
            return Merge.ensureOnHandler(plan, context);
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return invokeConsumingPlanner(relation, context);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, Planner.Context context) {
            context.applySoftLimit(table.querySpec());
            return super.visitQueriedTable(table, context);
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, Planner.Context context) {
            QuerySpec querySpec = table.querySpec();
            context.applySoftLimit(querySpec);
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return invokeConsumingPlanner(table, context);
            }
            // TODO: Remove ESGetStatementPlanner and create new consumer for nested queries.
            if (querySpec.where().docKeys().isPresent() &&
                !table.tableRelation().tableInfo().isAlias()) {
                SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
                Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(table.querySpec());
                return MultiPhasePlan.createIfNeeded(ESGetStatementPlanner.convert(table, context, clusterService), subQueries);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            Limits limits = context.getLimits(querySpec);
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && limits.finalLimit() == 0)) {
                return new NoopPlan(context.jobId());
            }
            return invokeConsumingPlanner(table, context);
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            QuerySpec querySpec = mss.querySpec();
            context.applySoftLimit(querySpec);
            if (!querySpec.hasAggregates() && querySpec.where().noMatch()) {
                return new NoopPlan(context.jobId());
            }
            return invokeConsumingPlanner(mss, context);
        }
    }
}
