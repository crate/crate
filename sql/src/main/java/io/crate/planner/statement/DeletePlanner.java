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

package io.crate.planner.statement;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.ParamSymbols;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.DeleteProjection;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.types.DataTypes;

import java.util.Collections;

import static java.util.Objects.requireNonNull;

public final class DeletePlanner {

    public static Plan planDelete(Functions functions, AnalyzedDeleteStatement delete, PlannerContext context) {
        DocTableRelation tableRel = delete.relation();
        DocTableInfo table = tableRel.tableInfo();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, delete.query(), table, context.transactionContext());

        if (!detailedQuery.partitions().isEmpty()) {
            return new DeletePartitions(context.jobId(), table.ident(), detailedQuery.partitions());
        }
        if (detailedQuery.docKeys().isPresent()) {
            return new DeleteById(context.jobId(), tableRel.tableInfo(), detailedQuery.docKeys().get());
        }
        Symbol query = detailedQuery.query();
        if (table.isPartitioned() && query instanceof Input && DataTypes.BOOLEAN.value(((Input) query).value())) {
            return new DeleteAllPartitions(
                context.jobId(), Lists2.copyAndReplace(table.partitions(), IndexParts::toIndexName));
        }
        return (plannerCtx, projectionBuilder, params) ->
            deleteByQuery(functions, tableRel, detailedQuery , context, params);
    }

    private static ExecutionPlan deleteByQuery(Functions functions,
                                               DocTableRelation table,
                                               WhereClauseOptimizer.DetailedQuery detailedQuery,
                                               PlannerContext context,
                                               Row params) {
        DocTableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID), "Table has to have a _id reference");
        DeleteProjection deleteProjection = new DeleteProjection(new InputColumn(0, idReference.valueType()));

        // TODO: change API so it does only the necessary partition logic
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, table);
        Symbol query = ParamSymbols.toLiterals(detailedQuery.query(), params);
        WhereClause where = whereClauseAnalyzer.analyze(query, context.transactionContext());

        SessionContext sessionContext = context.transactionContext().sessionContext();
        Routing routing = context.allocateRouting(
            tableInfo, where, RoutingProvider.ShardSelection.PRIMARIES, sessionContext);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            ImmutableList.of(idReference),
            ImmutableList.of(deleteProjection),
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            sessionContext.user()
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, context, Collections.singletonList(MergeCountProjection.INSTANCE));
    }
}
