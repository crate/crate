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

import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.ResultDescription;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public class DistributedGroupBy implements Plan {

    private final RoutedCollectPhase collectNode;
    private final MergePhase reducerMergeNode;
    private MergePhase localMergeNode;
    private final UUID id;

    public DistributedGroupBy(RoutedCollectPhase collectNode, MergePhase reducerMergeNode, @Nullable MergePhase localMergeNode, UUID id) {
        this.collectNode = collectNode;
        this.reducerMergeNode = reducerMergeNode;
        this.localMergeNode = localMergeNode;
        this.id = id;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitDistributedGroupBy(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public RoutedCollectPhase collectNode() {
        return collectNode;
    }

    public MergePhase reducerMergeNode() {
        return reducerMergeNode;
    }

    public MergePhase localMergeNode() {
        return localMergeNode;
    }

    @Override
    public void addProjection(Projection projection) {
        if (localMergeNode != null) {
            localMergeNode.addProjection(projection);
        } else {
            reducerMergeNode.addProjection(projection);
        }
    }

    @Override
    public ResultDescription resultDescription() {
        if (localMergeNode == null) {
            return reducerMergeNode;
        }
        return localMergeNode;
    }
}
