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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.TableStats;
import io.crate.planner.node.dql.Collect;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Get implements LogicalPlan {

    final DocTableRelation tableRelation;
    final DocKeys docKeys;
    final long estimatedSizePerRow;
    private final List<Symbol> outputs;

    public Get(DocTableRelation table, DocKeys docKeys, List<Symbol> outputs, TableStats tableStats) {
        this.outputs = outputs;
        this.tableRelation = table;
        this.docKeys = docKeys;
        this.estimatedSizePerRow = tableStats.estimatedSizePerRow(tableRelation.tableInfo().ident());
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        HashMap<String, Map<ShardId, List<PKAndVersion>>> idsByShardByNode = new HashMap<>();
        DocTableInfo docTableInfo = tableRelation.tableInfo();
        List<Symbol> boundOutputs = Lists2.map(
            outputs, s -> SubQueryAndParamBinder.convert(s, params, subQueryResults));
        for (DocKeys.DocKey docKey : docKeys) {
            String id = docKey.getId(plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults);
            if (id == null) {
                continue;
            }
            List<String> partitionValues = docKey.getPartitionValues(
                plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults);
            String indexName = indexName(docTableInfo, partitionValues);

            String routing = docKey.getRouting(
                plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults);
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
                .version(plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults)
                .orElse(Versions.MATCH_ANY);
            long sequenceNumber = docKey.sequenceNo(plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = docKey.primaryTerm(plannerContext.transactionContext(), plannerContext.functions(), params, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            pkAndVersions.add(new PKAndVersion(id, version, sequenceNumber, primaryTerm));
        }
        return new Collect(
            new PKLookupPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                docTableInfo.partitionedBy(),
                boundOutputs,
                idsByShardByNode
            ),
            TopN.NO_LIMIT,
            0,
            boundOutputs.size(),
            docKeys.size(),
            null
        );
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return List.of(tableRelation);
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.isEmpty() : "Get has no sources, cannot replace them";
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long estimatedRowSize() {
        return estimatedSizePerRow;
    }

    @Override
    public long numExpectedRows() {
        return docKeys.size();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitGet(this, context);
    }

    public static String indexName(DocTableInfo tableInfo, @Nullable List<String> partitionValues) {
        RelationName relation = tableInfo.ident();
        if (tableInfo.isPartitioned()) {
            assert partitionValues != null : "values must not be null";
            return IndexParts.toIndexName(relation, PartitionName.encodeIdent(partitionValues));
        } else {
            return relation.indexNameOrAlias();
        }
    }
}
