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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Assignments;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.ParamSymbols;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.data.Row;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SysUpdateProjection;
import io.crate.planner.projection.UpdateProjection;

import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public final class UpdatePlanner {

    private UpdatePlanner() {
    }

    public static Plan plan(AnalyzedUpdateStatement update, Functions functions, PlannerContext plannerCtx) {
        AbstractTableRelation table = update.table();
        if (table instanceof DocTableRelation) {
            DocTableRelation docTable = (DocTableRelation) table;
            return plan(docTable, update.assignmentByTargetCol(), update.query(), functions, plannerCtx);
        }
        return (plannerContext, projectionBuilder, params) ->
            sysUpdate(plannerContext, (TableRelation) table, update.assignmentByTargetCol(), update.query(), params);
    }

    private static Plan plan(DocTableRelation docTable,
                             Map<Reference, Symbol> assignmentByTargetCol,
                             Symbol query,
                             Functions functions,
                             PlannerContext plannerCtx) {
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        DocTableInfo tableInfo = docTable.tableInfo();
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, query, tableInfo, plannerCtx.transactionContext());

        if (detailedQuery.docKeys().isPresent()) {
            return new UpdateById(
                plannerCtx.jobId(), tableInfo, assignmentByTargetCol, detailedQuery.docKeys().get());
        }

        return (plannerContext, projectionBuilder, params) ->
            updateByQuery(functions, plannerContext, docTable, assignmentByTargetCol, query, params);
    }

    private static ExecutionPlan sysUpdate(PlannerContext plannerContext,
                                           TableRelation table,
                                           Map<Reference, Symbol> assignmentByTargetCol,
                                           Symbol query,
                                           Row params) {
        TableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID), "Table must have a _id column");
        SysUpdateProjection updateProjection = new SysUpdateProjection(idReference.valueType(), assignmentByTargetCol);
        WhereClause where = new WhereClause(ParamSymbols.toLiterals(query, params));
        return createCollectAndMerge(plannerContext, tableInfo, idReference, updateProjection, where);
    }

    private static ExecutionPlan updateByQuery(Functions functions,
                                               PlannerContext plannerCtx,
                                               DocTableRelation table,
                                               Map<Reference, Symbol> assignmentByTargetCol,
                                               Symbol query,
                                               Row params) {
        DocTableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID), "Table must have a _id column");
        Assignments assignments = Assignments.convert(assignmentByTargetCol);
        Symbol[] assignmentSources = assignments.bindSources(tableInfo, params);
        UpdateProjection updateProjection = new UpdateProjection(
            new InputColumn(0, idReference.valueType()), assignments.targetNames(), assignmentSources, null);
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, table);
        WhereClause where = whereClauseAnalyzer.analyze(
            ParamSymbols.toLiterals(query, params), plannerCtx.transactionContext());
        if (where.hasVersions()) {
            throw new VersionInvalidException();
        }
        return createCollectAndMerge(plannerCtx, tableInfo, idReference, updateProjection, where);
    }

    private static ExecutionPlan createCollectAndMerge(PlannerContext plannerCtx,
                                                       TableInfo tableInfo,
                                                       Reference idReference,
                                                       Projection updateProjection,
                                                       WhereClause where) {
        SessionContext sessionContext = plannerCtx.transactionContext().sessionContext();
        Routing routing = plannerCtx.allocateRouting(
            tableInfo, where, RoutingProvider.ShardSelection.PRIMARIES, sessionContext);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            plannerCtx.jobId(),
            plannerCtx.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            newArrayList(idReference),
            singletonList(updateProjection),
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            plannerCtx.transactionContext().sessionContext().user()
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, plannerCtx, singletonList(MergeCountProjection.INSTANCE));
    }
}
