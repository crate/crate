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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.DocKeys;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static io.crate.data.SentinelRow.SENTINEL;


public class ESGet implements Plan, ExecutionPlan {

    private final DocTableInfo tableInfo;
    private final List<Symbol> sortSymbols;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;
    private final int executionPhaseId;
    private final int offset;

    private static final boolean[] EMPTY_REVERSE_FLAGS = new boolean[0];
    private static final Boolean[] EMPTY_NULLS_FIRST = new Boolean[0];
    private final DocKeys docKeys;
    private final List<Symbol> outputs;
    private final List<DataType> outputTypes;
    private final int limit;

    public ESGet(int executionPhaseId,
                 DocTableInfo tableInfo,
                 List<Symbol> outputs,
                 DocKeys docKeys,
                 @Nullable OrderBy orderBy,
                 int limit,
                 int offset) {
        this.tableInfo = tableInfo;
        this.outputs = outputs;
        this.docKeys = docKeys;
        this.executionPhaseId = executionPhaseId;
        this.offset = offset;
        this.limit = limit;

        outputTypes = Symbols.typeView(outputs);

        if (orderBy == null) {
            this.sortSymbols = ImmutableList.of();
            this.reverseFlags = EMPTY_REVERSE_FLAGS;
            this.nullsFirst = EMPTY_NULLS_FIRST;
        } else {
            this.sortSymbols = orderBy.orderBySymbols();
            this.reverseFlags = orderBy.reverseFlags();
            this.nullsFirst = orderBy.nullsFirst();
        }
    }

    public List<DataType> outputTypes() {
        return outputTypes;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public List<Symbol> sortSymbols() {
        return sortSymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("docKeys", docKeys)
            .add("outputs", outputs)
            .toString();
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        if (limit == 0) {
            consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
            return;
        }
        ESGetTask esGetTask = new ESGetTask(
            plannerContext.jobId(),
            executor.functions(),
            executor.projectorFactory(),
            executor.transportActionProvider(),
            this,
            executor.jobContextService()
        );
        esGetTask.execute(consumer);
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitESGet(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("Cannot add projections to ESGet");
    }

    @Override
    public void addProjection(Projection projection, int unfinishedLimit, int unfinishedOffset, @Nullable PositionalOrderBy unfinishedOrderBy) {
        throw new UnsupportedOperationException("Cannot add projections to ESGet");
    }

    @Override
    public ResultDescription resultDescription() {
        throw new UnsupportedOperationException("ESGet has no resultDescription");
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        throw new UnsupportedOperationException("Cannot set distributionInfo on ESGet");
    }
}
