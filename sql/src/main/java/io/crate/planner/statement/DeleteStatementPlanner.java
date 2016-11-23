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
import io.crate.analyze.DeleteAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Merge;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ddl.ESDeletePartition;
import io.crate.planner.node.dml.Delete;
import io.crate.planner.node.dml.ESDelete;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.DeleteProjection;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.Preference;

import java.util.*;

public final class DeleteStatementPlanner {

    public static Plan planDelete(DeleteAnalyzedStatement analyzedStatement, Planner.Context context) {
        DocTableRelation tableRelation = analyzedStatement.analyzedRelation();
        List<WhereClause> whereClauses = new ArrayList<>(analyzedStatement.whereClauses().size());
        List<DocKeys.DocKey> docKeys = new ArrayList<>(analyzedStatement.whereClauses().size());

        Map<Integer, Integer> itemToBulkIdx = new HashMap<>();
        int bulkIdx = -1;
        int itemIdx = 0;
        for (WhereClause whereClause : analyzedStatement.whereClauses()) {
            bulkIdx++;
            if (whereClause.noMatch()) {
                continue;
            }
            if (whereClause.docKeys().isPresent() && whereClause.docKeys().get().size() == 1) {
                docKeys.add(whereClause.docKeys().get().getOnlyKey());
                itemToBulkIdx.put(itemIdx, bulkIdx);
                itemIdx++;
            } else if (!whereClause.noMatch()) {
                whereClauses.add(whereClause);
            }
        }

        if (!docKeys.isEmpty()) {
            return new ESDelete(context.jobId(),
                context.nextExecutionPhaseId(),
                tableRelation.tableInfo(),
                docKeys,
                itemToBulkIdx,
                analyzedStatement.whereClauses().size());
        } else if (!whereClauses.isEmpty()) {
            return deleteByQuery(tableRelation.tableInfo(), whereClauses, context);
        }

        return new NoopPlan(context.jobId());
    }

    private static Plan deleteByQuery(DocTableInfo tableInfo,
                               List<WhereClause> whereClauses,
                               Planner.Context context) {

        List<Plan> planNodes = new ArrayList<>();
        List<String> indicesToDelete = new ArrayList<>();

        for (WhereClause whereClause : whereClauses) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            if (indices.length > 0) {
                if (!whereClause.hasQuery() && tableInfo.isPartitioned()) {
                    indicesToDelete.addAll(Arrays.asList(indices));
                } else {
                    planNodes.add(collectWithDeleteProjection(tableInfo, whereClause, context));
                }
            }
        }
        if (!indicesToDelete.isEmpty()) {
            assert planNodes.isEmpty() : "If a partition can be deleted that must be true for all bulk operations";
            return new ESDeletePartition(context.jobId(), indicesToDelete.toArray(new String[0]));
        }
        if (planNodes.isEmpty()) {
            return new NoopPlan(context.jobId());
        }
        return new Delete(planNodes, context.jobId());
    }

    private static Plan collectWithDeleteProjection(TableInfo tableInfo,
                                             WhereClause whereClause,
                                             Planner.Context plannerContext) {
        // for delete, we always need to collect the `_uid`
        Reference uidReference = new Reference(
            new ReferenceIdent(tableInfo.ident(), "_uid"), RowGranularity.DOC, DataTypes.STRING);

        DeleteProjection deleteProjection = new DeleteProjection(
            new InputColumn(0, DataTypes.STRING));

        Routing routing = plannerContext.allocateRouting(tableInfo, whereClause, Preference.PRIMARY.type());
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            ImmutableList.<Symbol>of(uidReference),
            ImmutableList.<Projection>of(deleteProjection),
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST,
            (byte) 0);
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, plannerContext, Collections.singletonList(MergeCountProjection.INSTANCE));
    }
}
