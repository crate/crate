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
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.Reference;
import io.crate.metadata.TableIdent;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.fetch.MultiSourceFetchPushDown;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Map;

@Singleton
public class SelectStatementPlanner {

    private final Visitor visitor;

    @Inject
    public SelectStatementPlanner(ClusterService clusterService, ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(clusterService, consumingPlanner);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<Planner.Context, Plan> {

        private final ClusterService clusterService;
        private final ConsumingPlanner consumingPlanner;

        public Visitor(ClusterService clusterService, ConsumingPlanner consumingPlanner) {
            this.clusterService = clusterService;
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return consumingPlanner.plan(relation, context);
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
            if (plannedSubQuery == null) {
                return null;
            }
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
                    pd.fetchSources(),
                    pd.remainingOutputs(),
                    readerAllocations.nodeReaders(),
                    readerAllocations.indices(),
                    readerAllocations.indicesToIdents());

            plannedSubQuery.addProjection(fp);
            return new QueryThenFetch(plannedSubQuery.plan(), fetchPhase, null, context.jobId());
        }
    }
}
