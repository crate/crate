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


import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.collections.Lists2;
import io.crate.exceptions.VersionInvalidException;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.PrimaryKeyLookupPhase;
import io.crate.planner.node.dql.PrimaryKeyLookupPlan;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

public class ESGetStatementPlanner {

    public static Plan convert(QueriedDocTable table, Planner.Context context, ClusterService clusterService) {
        QuerySpec querySpec = table.querySpec();
        Optional<DocKeys> optKeys = querySpec.where().docKeys();
        assert !querySpec.hasAggregates() : "Can't create ESGet plan for queries with aggregates";
        assert !querySpec.groupBy().isPresent() : "Can't create ESGet plan for queries with group by";
        assert optKeys.isPresent() : "Can't create ESGet without docKeys";

        DocTableInfo tableInfo = table.tableRelation().tableInfo();
        DocKeys docKeys = optKeys.get();
        if (docKeys.withVersions()){
            throw new VersionInvalidException();
        }

        Optional<OrderBy> optOrderBy = querySpec.orderBy();
        List<Symbol> qsOutputs = querySpec.outputs();
        List<Symbol> toCollect = getToCollectSymbols(qsOutputs, optOrderBy);
        Limits limits = context.getLimits(querySpec);
        if (limits.hasLimit() && limits.finalLimit() == 0) {
            return new NoopPlan(context.jobId());
        }
        table.tableRelation().validateOrderBy(querySpec.orderBy());

        Projection topNOrEval = ProjectionBuilder.topNOrEval(
            toCollect,
            optOrderBy.orElse(null),
            limits.offset(),
            limits.finalLimit(),
            querySpec.outputs()
        );

        HashMap<Integer, List<DocKeys.DocKey>> docKeysByShard = docKeysByShard(
            clusterService,
            docKeys,
            tableInfo
        );

        Map<ColumnIdent, Integer> pkMapping = retrievePKMapping(toCollect, tableInfo);

        Routing routing = context.allocateRouting(tableInfo, querySpec.where(), Preference.PRIMARY.type());

        PrimaryKeyLookupPhase primaryKeyLookupPhase = new PrimaryKeyLookupPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "primary-key-lookup",
            routing, // FIXME: replace with createDocKeysByShardId(),
            tableInfo.rowGranularity(),
            pkMapping,
            toCollect,
            Collections.singletonList(topNOrEval),
            docKeys,
            docKeysByShard,
            DistributionInfo.DEFAULT_BROADCAST
        );

        PrimaryKeyLookupPlan plan = new PrimaryKeyLookupPlan(
            primaryKeyLookupPhase,
            limits.finalLimit(),
            limits.offset(),
            qsOutputs.size(),
            limits.limitAndOffset(),
            PositionalOrderBy.of(optOrderBy.orElse(null), toCollect)
        );

        return Merge.ensureOnHandler(plan, context);
    }

    public static Map<ColumnIdent, Integer> retrievePKMapping(List<Symbol> toCollect, DocTableInfo docTableInfo) {
        Map<ColumnIdent, Integer> result = new HashMap<>();
        for (Symbol symbol : toCollect) {
            ColumnIdent columnIdent = ((Reference) symbol).ident().columnIdent();
            if (docTableInfo.isPartitioned() && docTableInfo.partitionedBy().contains(columnIdent)) {
                int pkPos = docTableInfo.primaryKey().indexOf(columnIdent);
                if (pkPos >= 0) {
                    result.put(columnIdent, pkPos);
                }
            }
        }
        return result;
    }

    /**
     * @return qsOutputs + symbols from orderBy which are not already within qsOutputs (if orderBy is present)
     */
    private static List<Symbol> getToCollectSymbols(List<Symbol> qsOutputs, Optional<OrderBy> optOrderBy) {
        if (optOrderBy.isPresent()) {
            return Lists2.concatUnique(qsOutputs, optOrderBy.get().orderBySymbols());
        }
        return qsOutputs;
    }

    private static HashMap<Integer, List<DocKeys.DocKey>> docKeysByShard(
        ClusterService clusterService,
        DocKeys docKeys,
        DocTableInfo tableInfo) {
        HashMap<Integer, List<DocKeys.DocKey>> result = new HashMap();
        ClusterState state = clusterService.state();
        for (DocKeys.DocKey docKey: docKeys) {
            String indexName = ESGetTask.indexName(
                tableInfo.isPartitioned(),
                tableInfo.ident(),
                docKey.partitionValues().orElse(null)
            );
            try {
                ShardIterator shards = clusterService.operationRouting().getShards(
                    state,
                    indexName,
                    docKey.id(),
                    docKey.routing(),
                    Preference.PRIMARY.type()
                );
                ShardId shardId = shards.shardId();
                Integer id = shardId.getId();
                List<DocKeys.DocKey> resultDocKeys = result.get(id);
                if (resultDocKeys == null) {
                    resultDocKeys = new ArrayList<>();
                    result.put(id, resultDocKeys);
                }
                resultDocKeys.add(docKey);
            } catch (IndexNotFoundException e) {
                if (tableInfo.isPartitioned()) {
                    continue;
                }
                throw e;
            }
        }
        return result;
    }
}
