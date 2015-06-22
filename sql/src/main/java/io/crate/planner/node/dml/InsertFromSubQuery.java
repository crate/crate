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

package io.crate.planner.node.dml;


import com.google.common.base.Optional;
import io.crate.planner.Plan;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public class InsertFromSubQuery extends PlanAndPlannedAnalyzedRelation {


    private final Optional<MergeNode> handlerMergeNode;

    private final Plan innerPlan;

    private final UUID id;

    public InsertFromSubQuery(Plan innerPlan, @Nullable MergeNode handlerMergeNode, UUID id) {
        this.innerPlan = innerPlan;
        this.handlerMergeNode = Optional.fromNullable(handlerMergeNode);
        this.id = id;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitInsertByQuery(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public Plan innerPlan() {
        return innerPlan;
    }

    public Optional<MergeNode> handlerMergeNode() {
        return handlerMergeNode;
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addingProjection not supported");
    }

    @Override
    public boolean resultIsDistributed() {
        throw new UnsupportedOperationException("resultIsDistributed is not supported");
    }

    @Override
    public DQLPlanNode resultNode() {
        throw new UnsupportedOperationException("resultNode is not supported");
    }
}
