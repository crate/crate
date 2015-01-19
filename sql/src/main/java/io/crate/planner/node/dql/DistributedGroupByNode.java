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

package io.crate.planner.node.dql;

import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class DistributedGroupByNode implements PlannedAnalyzedRelation, PlanNode {

    private final CollectNode collectNode;
    private final MergeNode reducerMergeNode;
    private final MergeNode localMergeNode;

    public DistributedGroupByNode(CollectNode collectNode, MergeNode reducerMergeNode, MergeNode localMergeNode) {
        this.collectNode = collectNode;
        this.reducerMergeNode = reducerMergeNode;
        this.localMergeNode = localMergeNode;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField is not supported");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField is not supported");
    }

    @Override
    public List<Field> fields() {
        throw new UnsupportedOperationException("fields is not supported");
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitDistributedGroupByNode(this, context);
    }

    @Override
    public List<DataType> outputTypes() {
        return localMergeNode.outputTypes();
    }

    @Override
    public void outputTypes(List<DataType> outputTypes) {
        throw new UnsupportedOperationException("set outputTypes is not supported");
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection not supported");
    }

    public MergeNode reducerMergeNode() {
        return reducerMergeNode;
    }

    public MergeNode localMergeNode() {
        return localMergeNode;
    }
}
