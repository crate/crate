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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Get extends ZeroInputPlan {

    final DocTableRelation tableRelation;
    final DocKeys docKeys;

    Get(QueriedDocTable table, DocKeys docKeys, List<Symbol> outputs) {
        super(outputs, Collections.singletonList(table.tableRelation()));
        this.tableRelation = table.tableRelation();
        this.docKeys = docKeys;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        HashMap<String, Map<ShardId, List<PKAndVersion>>> idsByShardByNode = new HashMap<>();
        DocTableInfo docTableInfo = tableRelation.tableInfo();
        for (DocKeys.DocKey docKey : docKeys) {
            String id = docKey.getId(plannerContext.functions(), params, subQueryValues);
            if (id == null) {
                continue;
            }
            List<BytesRef> partitionValues = docKey.getPartitionValues(plannerContext.functions(), params, subQueryValues);
            String indexName = indexName(docTableInfo, partitionValues);

            String routing = docKey.getRouting(plannerContext.functions(), params, subQueryValues);
            ShardRouting shardRouting;
            try {
                shardRouting = plannerContext.resolveShard(indexName, id, routing);
            } catch (IndexNotFoundException e) {
                if (docTableInfo.isPartitioned()) {
                    continue;
                }
                throw e;
            }
            String currentNodeId = shardRouting.currentNodeId();
            if (currentNodeId == null) {
                // If relocating is fast enough this will work, otherwise it will result in a shard failure which
                // will cause a statement retry
                currentNodeId = shardRouting.relocatingNodeId();
                if (currentNodeId == null) {
                    throw new ShardNotFoundException(shardRouting.shardId());
                }
            }
            Map<ShardId, List<PKAndVersion>> idsByShard = idsByShardByNode.get(currentNodeId);
            if (idsByShard == null) {
                idsByShard = new HashMap<>();
                idsByShardByNode.put(currentNodeId, idsByShard);
            }
            List<PKAndVersion> pkAndVersions = idsByShard.get(shardRouting.shardId());
            if (pkAndVersions == null) {
                pkAndVersions = new ArrayList<>();
                idsByShard.put(shardRouting.shardId(), pkAndVersions);
            }
            long version = docKey
                .version(plannerContext.functions(), params, subQueryValues)
                .orElse(Versions.MATCH_ANY);
            pkAndVersions.add(new PKAndVersion(id, version));
        }
        return new Collect(
            new PKLookupPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                docTableInfo.partitionedBy(),
                outputs,
                idsByShardByNode
            ),
            TopN.NO_LIMIT,
            0,
            outputs.size(),
            docKeys.size(),
            null
        );
    }

    @Override
    public long numExpectedRows() {
        return docKeys.size();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitGet(this, context);
    }

    public static String indexName(DocTableInfo tableInfo, @Nullable List<BytesRef> partitionValues) {
        if (tableInfo.isPartitioned()) {
            assert partitionValues != null : "values must not be null";
            return new PartitionName(tableInfo.ident(), partitionValues).asIndexName();
        } else {
            return tableInfo.ident().indexName();
        }
    }
}
