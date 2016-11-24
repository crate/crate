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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.exceptions.ValidationException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.ESGetStatementPlanner;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.fetch.MultiSourceFetchPushDown;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;

import java.util.ArrayList;
import java.util.Map;

class SelectStatementPlanner {

    private final Visitor visitor;

    SelectStatementPlanner(ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(consumingPlanner);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static Plan subPlan(AnalyzedRelation rel, Planner.Context context) {
        ConsumerContext consumerContext = new ConsumerContext(rel, context);
        Plan subPlan = context.planSubRelation(rel, consumerContext);
        assert subPlan != null : "plan must not be null";
        ValidationException validationException = consumerContext.validationException();
        if (validationException != null) {
            throw validationException;
        }
        return subPlan;
    }

    private static class Visitor extends AnalyzedRelationVisitor<Planner.Context, Plan> {

        private final ConsumingPlanner consumingPlanner;

        public Visitor(ConsumingPlanner consumingPlanner) {
            this.consumingPlanner = consumingPlanner;
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
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
                Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(table.querySpec());
                return MultiPhasePlan.createIfNeeded(ESGetStatementPlanner.convert(table, context), subQueries);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            Limits limits = context.getLimits(querySpec);
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && limits.finalLimit() == 0)) {
                return new NoopPlan(context.jobId());
            }

            FetchPushDown fetchPushDown = new FetchPushDown(querySpec, table.tableRelation(), table.relationId());
            QueriedDocTable subRelation = fetchPushDown.pushDown();
            if (subRelation == null) {
                return invokeConsumingPlanner(table, context);
            }
            Plan plannedSubQuery = subPlan(subRelation, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            plannedSubQuery = Merge.ensureOnHandler(plannedSubQuery, context);

            Planner.Context.ReaderAllocations readerAllocations = context.buildReaderAllocations();

            FetchPhase fetchPhase = new FetchPhase(
                context.nextExecutionPhaseId(),
                readerAllocations.nodeReaders().keySet(),
                readerAllocations.bases(),
                readerAllocations.tableIndices(),
                fetchPushDown.fetchRefs()
            );
            FetchProjection fp = createFetchProjection(
                table, querySpec, fetchPushDown, readerAllocations, fetchPhase, context.fetchSize());
            plannedSubQuery.addProjection(fp, null, null, fp.outputs().size(), null);

            QueryThenFetch qtf = new QueryThenFetch(plannedSubQuery, fetchPhase);
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            Map<Plan, SelectSymbol> subqueries = subqueryPlanner.planSubQueries(querySpec);
            return MultiPhasePlan.createIfNeeded(qtf, subqueries);
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            QuerySpec querySpec = mss.querySpec();
            context.applySoftLimit(querySpec);
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return invokeConsumingPlanner(mss, context);
            }
            if (querySpec.where().noMatch() && !querySpec.hasAggregates()) {
                return new NoopPlan(context.jobId());
            }
            if (mss.canBeFetched().isEmpty()) {
                return invokeConsumingPlanner(mss, context);
            }
            MultiSourceFetchPushDown pd = MultiSourceFetchPushDown.pushDown(mss);
            Plan plannedSubQuery = invokeConsumingPlanner(mss, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";

            Planner.Context.ReaderAllocations readerAllocations = context.buildReaderAllocations();
            ArrayList<Reference> docRefs = new ArrayList<>();
            for (Map.Entry<TableIdent, FetchSource> entry : pd.fetchSources().entrySet()) {
                docRefs.addAll(entry.getValue().references());
            }

            FetchPhase fetchPhase = new FetchPhase(
                context.nextExecutionPhaseId(),
                readerAllocations.nodeReaders().keySet(),
                readerAllocations.bases(),
                readerAllocations.tableIndices(),
                docRefs
            );
            FetchProjection fp = new FetchProjection(
                fetchPhase.phaseId(),
                context.fetchSize(),
                pd.fetchSources(),
                pd.remainingOutputs(),
                readerAllocations.nodeReaders(),
                readerAllocations.indices(),
                readerAllocations.indicesToIdents());

            plannedSubQuery.addProjection(fp, null, null, fp.outputs().size(), null);
            return new QueryThenFetch(plannedSubQuery, fetchPhase);
        }
    }

    private static FetchProjection createFetchProjection(QueriedDocTable table,
                                                         QuerySpec querySpec,
                                                         FetchPushDown fetchPushDown,
                                                         Planner.Context.ReaderAllocations readerAllocations,
                                                         FetchPhase fetchPhase,
                                                         int fetchSize) {
        Map<TableIdent, FetchSource> fetchSources = ImmutableMap.of(table.tableRelation().tableInfo().ident(),
            new FetchSource(
                table.tableRelation().tableInfo().partitionedByColumns(),
                ImmutableList.of(fetchPushDown.fetchIdCol()),
                fetchPushDown.fetchRefs()));

        return new FetchProjection(
            fetchPhase.phaseId(),
            fetchSize,
            fetchSources,
            querySpec.outputs(),
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents());
    }
}
