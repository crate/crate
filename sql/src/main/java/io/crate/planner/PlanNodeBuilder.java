/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class PlanNodeBuilder {

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       List<Symbol> toCollect,
                                       List<Projection> projections,
                                       @Nullable String partitionIdent,
                                       @Nullable String routingPreference,
                                       @Nullable OrderBy orderBy,
                                       @Nullable Integer limit) {
        Routing routing = tableInfo.getRouting(whereClause, routingPreference);
        return collect(jobId, tableInfo, plannerContext, whereClause, routing, toCollect, projections, partitionIdent, orderBy, limit);
    }

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       Routing routing,
                                       List<Symbol> toCollect,
                                       List<Projection> projections,
                                       @Nullable String partitionIdent,
                                       @Nullable OrderBy orderBy,
                                       @Nullable Integer limit) {
        assert !Iterables.any(toCollect, Predicates.instanceOf(InputColumn.class)) : "cannot collect inputcolumns";
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                    tableInfo.ident().schema(), tableInfo.ident().name(), partitionIdent).stringValue());
        }
        plannerContext.allocateJobSearchContextIds(routing);
        CollectPhase node = new CollectPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "collect",
                routing,
                tableInfo.rowGranularity(),
                toCollect,
                projections,
                whereClause);
        node.orderBy(orderBy);
        node.limit(limit);
        return node;
    }

    private static Routing filterRouting(Routing routing, String includeTableName) {
        assert routing.hasLocations();
        assert includeTableName != null;
        Map<String, Map<String, List<Integer>>> newLocations = new TreeMap<>();

        for (Map.Entry<String, Map<String, List<Integer>>> entry : routing.locations().entrySet()) {
            Map<String, List<Integer>> tableMap = new TreeMap<>();
            for (Map.Entry<String, List<Integer>> tableEntry : entry.getValue().entrySet()) {
                if (includeTableName.equals(tableEntry.getKey())) {
                    tableMap.put(tableEntry.getKey(), tableEntry.getValue());
                }
            }
            if (tableMap.size() > 0) {
                newLocations.put(entry.getKey(), tableMap);
            }

        }
        if (newLocations.size() > 0) {
            return new Routing(newLocations);
        } else {
            return new Routing();
        }

    }

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       List<Symbol> toCollect,
                                       ImmutableList<Projection> projections) {
        return collect(jobId, tableInfo, plannerContext, whereClause, toCollect, projections, null, null, null, null);
    }

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       List<Symbol> toCollect,
                                       ImmutableList<Projection> projections,
                                       @Nullable String partitionIdent,
                                       @Nullable String routingPreference) {
        return collect(jobId, tableInfo, plannerContext, whereClause, toCollect, projections, partitionIdent, routingPreference, null, null);
    }

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       List<Symbol> toCollect,
                                       ImmutableList<Projection> projections,
                                       @Nullable String partitionIdent) {
        return collect(jobId, tableInfo, plannerContext, whereClause, toCollect, projections, partitionIdent, null);
    }

    public static CollectPhase collect(UUID jobId,
                                       TableInfo tableInfo,
                                       Planner.Context plannerContext,
                                       WhereClause whereClause,
                                       List<Symbol> toCollect,
                                       List<Projection> projections,
                                       @Nullable OrderBy orderBy,
                                       @Nullable Integer limit) {
        return collect(jobId, tableInfo, plannerContext, whereClause, toCollect, projections, null, null, orderBy, limit);
    }
}
