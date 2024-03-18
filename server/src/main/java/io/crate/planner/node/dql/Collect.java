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

package io.crate.planner.node.dql;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataType;

public class Collect implements ExecutionPlan, ResultDescription {

    private final CollectPhase collectPhase;

    private int unfinishedLimit;

    private int unfinishedOffset;

    private int numOutputs;

    private final int maxRowsPerNode;

    @Nullable
    private PositionalOrderBy orderBy;

    /**
     * @param unfinishedLimit the limit a parent must apply after a merge to get the correct result
     * @param unfinishedOffset the offset a parent must apply after a merge to get the correct result
     * <ul>
     * <li>If the data should be limited as part of the Collect, add a {@link LimitAndOffsetProjection}</li>
     * <li>If the limit of the {@link LimitAndOffsetProjection} is final, unfinishedLimit here
     *     should be set to NO_LIMIT (-1).</li>
     * <p>
     * See also: {@link ResultDescription}
     * </p>
     */
    public Collect(CollectPhase collectPhase,
                   int unfinishedLimit,
                   int unfinishedOffset,
                   int numOutputs,
                   int maxRowsPerNode,
                   @Nullable PositionalOrderBy orderBy) {
        this.collectPhase = collectPhase;
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.numOutputs = numOutputs;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
    }

    public CollectPhase collectPhase() {
        return collectPhase;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        collectPhase.addProjection(projection);
        numOutputs = projection.outputs().size();
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        collectPhase.addProjection(projection);
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.orderBy = unfinishedOrderBy;
        numOutputs = projection.outputs().size();
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        collectPhase.distributionInfo(distributionInfo);
    }

    @Override
    public Collection<String> nodeIds() {
        return collectPhase.nodeIds();
    }

    @Nullable
    @Override
    public PositionalOrderBy orderBy() {
        return orderBy;
    }

    @Override
    public int limit() {
        return unfinishedLimit;
    }

    @Override
    public int maxRowsPerNode() {
        return maxRowsPerNode;
    }

    @Override
    public int offset() {
        return unfinishedOffset;
    }

    @Override
    public int numOutputs() {
        return numOutputs;
    }

    @Override
    public boolean executesOnShard() {
        List<Projection> projections = collectPhase.projections();
        if (projections.isEmpty()) {
            // Can't apply shard projections if there is an ORDER BY
            // because we've to merge the result of the shards first.
            // See ShardCollectSource.createMultiShardScoreDocCollector
            return collectPhase instanceof RoutedCollectPhase routedPhase
                && routedPhase.routing().containsShards()
                && routedPhase.orderBy() == null;
        }
        Projection lastProjection = projections.get(projections.size() - 1);
        return lastProjection.requiredGranularity() == RowGranularity.SHARD;
    }

    @Override
    public List<DataType<?>> streamOutputs() {
        List<Projection> projections = collectPhase.projections();
        if (projections.isEmpty()) {
            return Symbols.typeView(collectPhase.toCollect());
        }
        Projection lastProjection = projections.get(projections.size() - 1);
        return Symbols.typeView(lastProjection.outputs());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Collect collect = (Collect) o;
        return unfinishedLimit == collect.unfinishedLimit &&
               unfinishedOffset == collect.unfinishedOffset &&
               numOutputs == collect.numOutputs &&
               maxRowsPerNode == collect.maxRowsPerNode &&
               Objects.equals(collectPhase, collect.collectPhase) &&
               Objects.equals(orderBy, collect.orderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectPhase, unfinishedLimit, unfinishedOffset, numOutputs, maxRowsPerNode, orderBy);
    }
}
