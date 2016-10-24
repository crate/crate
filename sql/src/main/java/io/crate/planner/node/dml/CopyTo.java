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

package io.crate.planner.node.dml;

import com.google.common.base.Optional;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;
import io.crate.planner.node.dql.MergePhase;

import javax.annotation.Nullable;
import java.util.UUID;

public class CopyTo extends UnnestablePlan {

    private final UUID id;
    private final Plan innerPlan;
    private final Optional<MergePhase> handlerMergeNode;

    public CopyTo(UUID id, Plan innerPlan, @Nullable MergePhase handlerMergeNode) {
        this.id = id;
        this.innerPlan = innerPlan;
        this.handlerMergeNode = Optional.fromNullable(handlerMergeNode);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCopyTo(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public Plan innerPlan() {
        return innerPlan;
    }

    public Optional<MergePhase> handlerMergeNode() {
        return handlerMergeNode;
    }
}
