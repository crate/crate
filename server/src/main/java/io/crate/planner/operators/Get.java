/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;

public class Get implements LogicalPlan {

    final DocTableRelation tableRelation;
    final DocKeys docKeys;
    final Symbol query;
    private final List<Symbol> outputs;
    private final boolean queryHasPkSymbolsOnly;

    public Get(DocTableRelation table,
               DocKeys docKeys,
               Symbol query,
               List<Symbol> outputs,
               boolean queryHasPkSymbolsOnly) {
        this.tableRelation = table;
        this.docKeys = docKeys;
        this.query = query;
        this.outputs = outputs;
        this.queryHasPkSymbolsOnly = queryHasPkSymbolsOnly;
    }

    @Override
    public boolean preferShardProjections() {
        return true;
    }

    public DocTableRelation table() {
        return tableRelation;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        HashMap<String, Map<ShardId, List<PKAndVersion>>> idsByShardByNode = new HashMap<>();
        DocTableInfo docTableInfo = tableRelation.tableInfo();
        for (DocKeys.DocKey docKey : docKeys) {
            String id = docKey.getId(plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults);
            if (id == null) {
                continue;
            }
            List<String> partitionValues = docKey.getPartitionValues(
                plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults);
            String indexName = indexName(docTableInfo, partitionValues);

            String routing = docKey.getRouting(
                plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults);
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
                .version(plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults)
                .orElse(Versions.MATCH_ANY);
            long sequenceNumber = docKey.sequenceNo(plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = docKey.primaryTerm(plannerContext.transactionContext(), plannerContext.nodeContext(), params, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            pkAndVersions.add(new PKAndVersion(id, version, sequenceNumber, primaryTerm));
        }

        var binder = new SubQueryAndParamBinder(params, subQueryResults);
        List<Symbol> boundOutputs = Lists.map(outputs, binder);
        var boundQuery = binder.apply(query);
        var toCollect = boundOutputs;
        ArrayList<Projection> projections = new ArrayList<>();
        if (!queryHasPkSymbolsOnly) {
            var toCollectSet = new LinkedHashSet<>(boundOutputs);
            boundQuery.visit(Reference.class, toCollectSet::add);
            toCollect = List.copyOf(toCollectSet);
            var filterProjection = ProjectionBuilder.filterProjection(toCollect, boundQuery);
            filterProjection.requiredGranularity(RowGranularity.SHARD);
            projections.add(filterProjection);

            // reduce outputs which have been added for the filter projection
            var evalProjection = new EvalProjection(InputColumn.mapToInputColumns(boundOutputs), RowGranularity.SHARD);
            projections.add(evalProjection);
        }

        var collect = new Collect(
            new PKLookupPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                docTableInfo.partitionedBy(),
                toCollect,
                idsByShardByNode
            ),
            LimitAndOffset.NO_LIMIT,
            0,
            toCollect.size(),
            docKeys.size(),
            null
        );
        for (var projection : projections) {
            collect.addProjection(projection);
        }

        return collect;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<RelationName> relationNames() {
        return List.of(tableRelation.relationName());
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
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        boolean excludedAny = false;
        for (Symbol output : outputs) {
            if (outputsToKeep.contains(output)) {
                newOutputs.add(output);
            } else {
                excludedAny = true;
            }
        }
        if (excludedAny) {
            return new Get(tableRelation, docKeys, query, newOutputs, queryHasPkSymbolsOnly);
        }
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

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
            return IndexName.encode(relation, PartitionName.encodeIdent(partitionValues));
        } else {
            return relation.indexNameOrAlias();
        }
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Get[")
            .text(tableRelation.tableInfo().ident().toString())
            .text(" | ")
            .text(Lists.joinOn(", ", outputs, Symbol::toString))
            .text(" | ")
            .text(docKeys.toString())
            .text(" | ")
            .text(query.toString())
            .text("]");
        printStats(printContext);
    }
}
