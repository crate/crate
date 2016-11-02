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
import io.crate.analyze.*;
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
import io.crate.sql.tree.QualifiedName;

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
            return Merge.mergeToHandler(consumingPlanner.plan(relation, context), context);
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return invokeConsumingPlanner(relation, context);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, Planner.Context context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            QuerySpec querySpec = table.querySpec();
            context.applySoftLimit(querySpec);
            Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(querySpec);
            Plan plan = super.visitQueriedTable(table, context);
            return MultiPhasePlan.createIfNeeded(plan, subQueries);
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, Planner.Context context) {
            QuerySpec querySpec = table.querySpec();
            context.applySoftLimit(querySpec);
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(querySpec);
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                Plan subPlan = invokeConsumingPlanner(table, context);
                return MultiPhasePlan.createIfNeeded(subPlan, subQueries);
            }
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                return MultiPhasePlan.createIfNeeded(ESGetStatementPlanner.convert(table, context), subQueries);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            Limits limits = context.getLimits(querySpec);
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && limits.finalLimit() == 0)) {
                return new NoopPlan(context.jobId());
            }
            table.tableRelation().validateOrderBy(querySpec.orderBy());

            FetchPushDown fetchPushDown = new FetchPushDown(querySpec, table.tableRelation());
            QueriedDocTable subRelation = fetchPushDown.pushDown();
            if (subRelation == null) {
                return MultiPhasePlan.createIfNeeded(invokeConsumingPlanner(table, context), subQueries);
            }
            Plan plannedSubQuery = subPlan(subRelation, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            plannedSubQuery = Merge.mergeToHandler(plannedSubQuery, context);

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

            QueryThenFetch queryThenFetch = new QueryThenFetch(plannedSubQuery, fetchPhase);
            return MultiPhasePlan.createIfNeeded(queryThenFetch, subQueries);
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            QuerySpec querySpec = mss.querySpec();
            context.applySoftLimit(querySpec);
            if (querySpec.where().noMatch() && !querySpec.hasAggregates()) {
                return new NoopPlan(context.jobId());
            }
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(querySpec);
            for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : mss.sources().entrySet()) {
                subQueries.putAll(subqueryPlanner.planSubQueries(entry.getValue().querySpec()));
            }
            if (mss.canBeFetched().isEmpty()) {
                return MultiPhasePlan.createIfNeeded(invokeConsumingPlanner(mss, context), subQueries);
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
            QueryThenFetch queryThenFetch = new QueryThenFetch(plannedSubQuery, fetchPhase);
            return MultiPhasePlan.createIfNeeded(Merge.mergeToHandler(queryThenFetch, context), subQueries);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, Planner.Context context) {
            throw new UnsupportedOperationException("complex sub selects are not supported");
        }
    }

    private static FetchProjection createFetchProjection(QueriedDocTable table,
                                                         QuerySpec querySpec,
                                                         FetchPushDown fetchPushDown,
                                                         Planner.Context.ReaderAllocations readerAllocations,
                                                         FetchPhase fetchPhase,
                                                         int fetchSize) {
        Map<TableIdent, FetchSource> fetchSources = ImmutableMap.of(table.tableRelation().tableInfo().ident(),
            new FetchSource(table.tableRelation().tableInfo().partitionedByColumns(),
                ImmutableList.of(fetchPushDown.docIdCol()),
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
