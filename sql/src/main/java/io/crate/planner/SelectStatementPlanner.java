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
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.ValidationException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.operation.projectors.TopN;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.ESGetStatementPlanner;
import io.crate.planner.consumer.SimpleSelect;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.fetch.MultiSourceFetchPushDown;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Map;

@Singleton
public class SelectStatementPlanner {

    private final Visitor visitor;

    @Inject
    public SelectStatementPlanner(ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(consumingPlanner);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static PlannedAnalyzedRelation subPlan(AnalyzedRelation rel, Planner.Context context) {
        ConsumerContext consumerContext = new ConsumerContext(rel, context);
        PlannedAnalyzedRelation subPlan =  context.planSubRelation(rel, consumerContext);
        assert subPlan != null;
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

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return consumingPlanner.plan(relation, context);
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, Planner.Context context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return consumingPlanner.plan(table, context);
            }
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                return ESGetStatementPlanner.convert(table, context);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && querySpec.limit().get() == 0)) {
                return new NoopPlan(context.jobId());
            }
            table.tableRelation().validateOrderBy(querySpec.orderBy());

            FetchPushDown fetchPushDown = new FetchPushDown(querySpec, table.tableRelation());
            QueriedDocTable subRelation = fetchPushDown.pushDown();
            if (subRelation == null) {
                return consumingPlanner.plan(table, context);
            }
            PlannedAnalyzedRelation plannedSubQuery = subPlan(subRelation, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";

            CollectAndMerge qaf = (CollectAndMerge) plannedSubQuery;
            RoutedCollectPhase collectPhase = ((RoutedCollectPhase) qaf.collectPhase());

            Planner.Context.Limits limits = context.getLimits(true, querySpec);
            if (collectPhase.nodePageSizeHint() == null && limits.limitAndOffset > TopN.NO_LIMIT) {
                collectPhase.nodePageSizeHint(limits.limitAndOffset);
            }

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

            MergePhase localMergePhase;
            assert qaf.localMerge() == null : "subRelation shouldn't plan localMerge";

            TopNProjection topN = ProjectionBuilder.topNProjection(
                    collectPhase.toCollect(),
                    null, // orderBy = null because stuff is pre-sorted in collectPhase and sortedLocalMerge is used
                    querySpec.offset(),
                    limits.finalLimit,
                    null
            );
            if (!querySpec.orderBy().isPresent()) {
                localMergePhase = MergePhase.localMerge(
                        context.jobId(),
                        context.nextExecutionPhaseId(),
                        ImmutableList.of(topN, fp),
                        collectPhase.executionNodes().size(),
                        collectPhase.outputTypes()
                );
            } else {
                localMergePhase = MergePhase.sortedMerge(
                        context.jobId(),
                        context.nextExecutionPhaseId(),
                        querySpec.orderBy().get(),
                        collectPhase.toCollect(),
                        null,
                        ImmutableList.of(topN, fp),
                        collectPhase.executionNodes().size(),
                        collectPhase.outputTypes()
                );
            }
            SimpleSelect.enablePagingIfApplicable(
                    collectPhase, localMergePhase, querySpec.limit().orNull(), querySpec.offset(),
                    context.clusterService().localNode().id());
            CollectAndMerge subPlan = new CollectAndMerge(collectPhase, null);
            return new QueryThenFetch(subPlan, fetchPhase, localMergePhase, context.jobId());
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            if (mss.querySpec().where().noMatch()) {
                return new NoopPlan(context.jobId());
            }
            if (mss.canBeFetched().isEmpty()){
                return consumingPlanner.plan(mss, context);
            }
            MultiSourceFetchPushDown pd = MultiSourceFetchPushDown.pushDown(mss);
            ConsumerContext consumerContext = new ConsumerContext(mss, context);
            // plan sub relation as if root so that it adds a mergePhase
            PlannedAnalyzedRelation plannedSubQuery = consumingPlanner.plan(mss, consumerContext);
            // NestedLoopConsumer can return NoopPlannedAnalyzedRelation if its left or right plan
            // is noop. E.g. it is the case with creating NestedLoopConsumer for empty partitioned tables.
            if (plannedSubQuery instanceof NoopPlannedAnalyzedRelation) {
                return new NoopPlan(context.jobId());
            }
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            assert !plannedSubQuery.resultIsDistributed() : "subQuery must not have a distributed result";

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
                fetchPhase.executionPhaseId(),
                context.fetchSize(),
                pd.fetchSources(),
                pd.remainingOutputs(),
                readerAllocations.nodeReaders(),
                readerAllocations.indices(),
                readerAllocations.indicesToIdents());

            plannedSubQuery.addProjection(fp);
            return new QueryThenFetch(plannedSubQuery.plan(), fetchPhase, null, context.jobId());
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
            fetchPhase.executionPhaseId(),
            fetchSize,
            fetchSources,
            querySpec.outputs(),
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents());
    }
}
