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

import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.VersionExtractor;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Assignments;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SysUpdateProjection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class UpdatePlanner {

    private static final  RelationVisitor RELATION_VISITOR = new RelationVisitor();

    private UpdatePlanner() {
    }

    public static Plan plan(UpdateAnalyzedStatement updateStatement) {
        return new UpdatePlan(updateStatement);
    }

    public static class UpdatePlan implements Plan {

        private final UpdateAnalyzedStatement update;

        public UpdatePlan(UpdateAnalyzedStatement update) {
            this.update = update;
        }

        @Override
        public ExecutionPlan build(PlannerContext plannerContext, ProjectionBuilder projectionBuilder, Row params) {
            return RELATION_VISITOR.process(update.sourceRelation(), new Context(update, plannerContext));
        }
    }

    static class Context {
        final UpdateAnalyzedStatement statement;
        private final PlannerContext plannerContext;

        public Context(UpdateAnalyzedStatement statement, PlannerContext plannerContext) {
            this.statement = statement;
            this.plannerContext = plannerContext;
        }
    }

    private static class RelationVisitor extends AnalyzedRelationVisitor<Context, ExecutionPlan> {

        @Override
        public ExecutionPlan visitDocTableRelation(DocTableRelation relation, Context context) {
            UpdateAnalyzedStatement statement = context.statement;
            PlannerContext plannerContext = context.plannerContext;

            DocTableRelation tableRelation = (DocTableRelation) statement.sourceRelation();
            DocTableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.isPartitioned() && tableInfo.partitions().isEmpty()) {
                return new NoopPlan(plannerContext.jobId());
            }

            List<ExecutionPlan> childNodes = new ArrayList<>(statement.nestedStatements().size());
            UpsertById upsertById = null;
            int bulkIdx = 0;
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
                WhereClause whereClause = nestedAnalysis.whereClause();
                if (whereClause.noMatch()) {
                    continue;
                }
                if (whereClause.docKeys().isPresent()) {
                    if (upsertById == null) {
                        Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());
                        int numBulkResponses = statement.nestedStatements().size();
                        if (numBulkResponses == 1) {
                            // disable bulk logic for 1 bulk item
                            numBulkResponses = 0;
                        }
                        upsertById = UpsertById.forUpdate(plannerContext.jobId(), numBulkResponses,
                            tableInfo.isPartitioned(), assignments.v1());
                    }
                    upsertById(nestedAnalysis, tableInfo, whereClause, upsertById, bulkIdx++);
                } else {
                    ExecutionPlan plan = upsertByQuery(nestedAnalysis, plannerContext, tableInfo, whereClause);
                    if (plan != null) {
                        childNodes.add(plan);
                    }
                }
            }
            if (upsertById != null) {
                assert childNodes.isEmpty() : "all bulk operations must resolve to the same sub-plan, either update-by-id or update-by-query";
                return upsertById;
            }
            return createUpsertPlan(childNodes, plannerContext.jobId());
        }

        @Override
        public ExecutionPlan visitTableRelation(TableRelation tableRelation, Context context) {
            UpdateAnalyzedStatement statement = context.statement;
            PlannerContext plannerContext = context.plannerContext;

            List<ExecutionPlan> childPlans = new ArrayList<>(statement.nestedStatements().size());
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedStatement : statement.nestedStatements()) {
                if (nestedStatement.whereClause().noMatch()) {
                    continue;
                }
                childPlans.add(createSysTableUpdatePlan(tableRelation.tableInfo(), plannerContext, nestedStatement));
            }
            return createUpsertPlan(childPlans, plannerContext.jobId());
        }
    }

    private static ExecutionPlan createUpsertPlan(List<ExecutionPlan> childPlans, UUID jobId) {
        if (childPlans.isEmpty()) {
            return new NoopPlan(jobId);
        }
        return new Upsert(childPlans, jobId);
    }

    private static ExecutionPlan createPlan(PlannerContext plannerContext,
                                            Routing routing,
                                            TableInfo tableInfo,
                                            Reference idReference,
                                            Projection updateProjection,
                                            WhereClause whereClause) {
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            Collections.singletonList(idReference),
            Collections.singletonList(updateProjection),
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST,
            plannerContext.transactionContext().sessionContext().user()
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, plannerContext, Collections.singletonList(MergeCountProjection.INSTANCE));
    }

    private static ExecutionPlan createSysTableUpdatePlan(TableInfo tableInfo,
                                                          PlannerContext plannerContext,
                                                          UpdateAnalyzedStatement.NestedAnalyzedStatement nestedStatement) {
        Routing routing = plannerContext.allocateRouting(
            tableInfo,
            nestedStatement.whereClause(),
            RoutingProvider.ShardSelection.PRIMARIES,
            plannerContext.transactionContext().sessionContext()
        );

        Reference idReference = tableInfo.getReference(DocSysColumns.ID);
        assert idReference != null : "table has no _id column";

        SysUpdateProjection updateProjection = new SysUpdateProjection(
            idReference.valueType(),
            nestedStatement.assignments());
        return createPlan(plannerContext, routing, tableInfo, idReference, updateProjection, nestedStatement.whereClause());
    }

    private static ExecutionPlan upsertByQuery(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                               PlannerContext plannerContext,
                                               DocTableInfo tableInfo,
                                               WhereClause whereClause) {

        Symbol versionSymbol = null;
        if (whereClause.hasVersions()) {
            VersionExtractor.Result result = VersionExtractor.extractVersionComparisons(whereClause.query());
            if (result.versions.size() > 1) {
                throw new UnsupportedOperationException("Only a single _version comparison may occur in the WHERE clause");
            }
            versionSymbol = result.versions.get(0);
            whereClause = new WhereClause(
                result.newQuery,
                whereClause.docKeys().orElse(null),
                whereClause.partitions(),
                whereClause.clusteredBy().orElse(null)
            );
        }

        if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
            // for updates, we always need to collect the `_id`
            Reference idReference = tableInfo.getReference(DocSysColumns.ID);

            Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());

            Long version = null;
            if (versionSymbol != null) {
                version = ValueSymbolVisitor.LONG.process(versionSymbol);
            }

            UpdateProjection updateProjection = new UpdateProjection(
                new InputColumn(0, DataTypes.STRING),
                assignments.v1(),
                assignments.v2(),
                version);
            Routing routing = plannerContext.allocateRouting(
                tableInfo,
                whereClause,
                RoutingProvider.ShardSelection.PRIMARIES,
                plannerContext.transactionContext().sessionContext());
            return createPlan(plannerContext, routing, tableInfo, idReference, updateProjection, whereClause);
        } else {
            return null;
        }
    }

    private static void upsertById(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                   DocTableInfo tableInfo,
                                   WhereClause whereClause,
                                   UpsertById upsertById,
                                   int bulkIdx) {
        String[] indices = Planner.indices(tableInfo, whereClause);
        assert tableInfo.isPartitioned() || indices.length == 1 :
            "table must be partitioned and number of indices should be 1";

        Tuple<String[], Symbol[]> assignments = Assignments.convert(nestedAnalysis.assignments());

        for (DocKeys.DocKey key : whereClause.docKeys().get()) {
            String index;
            if (key.partitionValues().isPresent()) {
                index = new PartitionName(tableInfo.ident(), key.partitionValues().get()).asIndexName();
            } else {
                index = indices[0];
            }
            upsertById.bulkIndices().add(bulkIdx);
            upsertById.add(
                index,
                key.id(),
                key.routing(),
                assignments.v2(),
                key.version().orElse(null));
        }
    }
}
