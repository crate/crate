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
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.DeleteProjection;
import io.crate.planner.projection.MergeCountProjection;

import java.util.Collections;

import static java.util.Objects.requireNonNull;

public final class DeletePlanner {

    public static Plan planDelete(Functions functions, AnalyzedDeleteStatement delete, Planner.Context context) {
        DocTableRelation tableRel = delete.relation();
        DocTableInfo table = tableRel.tableInfo();
        // TODO: drop all partitions on match-all
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, delete.query(), table, context.transactionContext());

        if (detailedQuery.docKeys().isPresent()) {
            return new DeleteById(context.jobId(), tableRel.tableInfo(), detailedQuery.docKeys().get());
        }
        if (!detailedQuery.partitions().isEmpty()) {
            return new DeletePartitions(context.jobId(), table.ident(), detailedQuery.partitions());
        }
        WhereClause where = new WhereClause(delete.query(), null, null, null);
        return deleteByQuery(tableRel.tableInfo(), where , context);
    }

    private static Plan deleteByQuery(DocTableInfo table, WhereClause where, Planner.Context context) {
        Reference idReference = requireNonNull(table.getReference(DocSysColumns.ID), "Table has to have a _id reference");
        DeleteProjection deleteProjection = new DeleteProjection(new InputColumn(0, idReference.valueType()));

        SessionContext sessionContext = context.transactionContext().sessionContext();
        Routing routing = context.allocateRouting(table, where, RoutingProvider.ShardSelection.PRIMARIES, sessionContext);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "collect",
            routing,
            table.rowGranularity(),
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
