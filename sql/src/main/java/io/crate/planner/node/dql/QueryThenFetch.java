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

import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;

import javax.annotation.Nullable;
import java.util.List;

public class QueryThenFetch implements PlannedAnalyzedRelation, Plan {

    private final CollectNode collectNode;
    private MergeNode mergeNode;

    public QueryThenFetch(CollectNode collectNode, MergeNode mergeNode) {
        this.collectNode = collectNode;
        this.mergeNode = mergeNode;
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    public MergeNode mergeNode() {
        return mergeNode;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryThenFetch(this, context);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField not supported on QueryThenFetch");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField not supported on QueryThenFetch");
    }

    @Override
    public List<Field> fields() {
        throw new UnsupportedOperationException("fields not supported on QueryThenFetch");
    }

    @Override
    public Plan plan() {
        return this;
    }

    @Override
    public void addProjection(Projection projection) {
        mergeNode.projections().add(projection);
    }

    @Override
    public boolean resultIsDistributed() {
        return mergeNode == null;
    }

    @Override
    public DQLPlanNode resultNode() {
        return mergeNode == null ? collectNode : mergeNode;
    }

}
