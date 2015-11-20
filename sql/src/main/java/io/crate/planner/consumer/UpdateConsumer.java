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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.VersionRewriter;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.*;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dml.UpsertByIdNode;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class UpdateConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public UpdateConsumer() {
        visitor = new Visitor();
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    static class Visitor extends RelationPlanningVisitor {

        @Override
        public PlannedAnalyzedRelation visitUpdateAnalyzedStatement(UpdateAnalyzedStatement statement, ConsumerContext context) {

            assert statement.sourceRelation() instanceof DocTableRelation : "sourceRelation of update statement must be a DocTableRelation";
            DocTableRelation tableRelation = (DocTableRelation) statement.sourceRelation();
            DocTableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            List<Plan> childNodes = new ArrayList<>(statement.nestedStatements().size());
            UpsertByIdNode upsertByIdNode = null;
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
                WhereClause whereClause = nestedAnalysis.whereClause();
                if (whereClause.noMatch()){
                    continue;
                }
                if (whereClause.docKeys().isPresent()) {
                    if (upsertByIdNode == null) {
                        Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());
                        upsertByIdNode = new UpsertByIdNode(context.plannerContext().nextExecutionPhaseId(), false, statement.nestedStatements().size() > 1, assignments.v1(), null);
                        childNodes.add(new IterablePlan(context.plannerContext().jobId(), upsertByIdNode));
                    }
                    upsertById(nestedAnalysis, tableInfo, whereClause, upsertByIdNode);
                } else {
                    Plan plan = upsertByQuery(nestedAnalysis, context, tableInfo, whereClause);
                    if (plan != null) {
                        childNodes.add(plan);
                    }
                }
            }
            if (childNodes.size() > 0){
                return new Upsert(childNodes, context.plannerContext().jobId());
            } else {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }
        }

        private Plan upsertByQuery(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                   ConsumerContext consumerContext,
                                   DocTableInfo tableInfo,
                                   WhereClause whereClause) {

            Symbol versionSymbol = null;
            if(whereClause.hasVersions()){
                versionSymbol = VersionRewriter.get(whereClause.query());
                whereClause = new WhereClause(whereClause.query(), whereClause.docKeys().orNull(), whereClause.partitions());
            }


            if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
                // for updates, we always need to collect the `_uid`
                Reference uidReference = new Reference(
                        new ReferenceInfo(
                                new ReferenceIdent(tableInfo.ident(), "_uid"),
                                RowGranularity.DOC, DataTypes.STRING));

                Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());

                Long version = null;
                if (versionSymbol != null){
                    version = ValueSymbolVisitor.LONG.process(versionSymbol);
                }

                UpdateProjection updateProjection = new UpdateProjection(
                        new InputColumn(0, DataTypes.STRING),
                        assignments.v1(),
                        assignments.v2(),
                        version);

                Planner.Context plannerContext = consumerContext.plannerContext();
                Routing routing = plannerContext.allocateRouting(tableInfo, whereClause, Preference.PRIMARY.type());
                CollectPhase collectPhase = new CollectPhase(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        "collect",
                        routing,
                        tableInfo.rowGranularity(),
                        ImmutableList.<Symbol>of(uidReference),
                        ImmutableList.<Projection>of(updateProjection),
                        whereClause,
                        DistributionInfo.DEFAULT_BROADCAST
                );
                MergePhase mergeNode = MergePhase.localMerge(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION),
                        collectPhase.executionNodes().size(),
                        collectPhase.outputTypes()
                );
                return new CollectAndMerge(collectPhase, mergeNode, plannerContext.jobId());
            } else {
                return null;
            }
        }

        private void upsertById(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                             DocTableInfo tableInfo,
                                             WhereClause whereClause,
                                             UpsertByIdNode upsertByIdNode) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            assert tableInfo.isPartitioned() || indices.length == 1;

            Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());

            for (DocKeys.DocKey key : whereClause.docKeys().get()) {
                String index;
                if (key.partitionValues().isPresent()) {
                    index = new PartitionName(tableInfo.ident(), key.partitionValues().get()).asIndexName();
                } else {
                    index = indices[0];
                }
                upsertByIdNode.add(
                        index,
                        key.id(),
                        key.routing(),
                        assignments.v2(),
                        key.version().orNull());
            }
        }
    }
}
