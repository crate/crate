/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.iterative;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.Function;

import io.crate.common.collections.Lists2;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;

class ResolvingVisitor extends LogicalPlanVisitor<Void, LogicalPlan> {
    private final Function<LogicalPlan, LogicalPlan> resolvePlan;

    public ResolvingVisitor(Function<LogicalPlan, LogicalPlan> resolvePlan) {
        this.resolvePlan = requireNonNull(resolvePlan, "resolvePlan is null");
    }

    @Override
    public LogicalPlan visitPlan(LogicalPlan node, Void context) {
        List<LogicalPlan> children = Lists2.mapLazy(node.sources(), child -> child.accept(this, context));
        return node.replaceSources(children);
    }

    @Override
    public LogicalPlan visitGroupReference(GroupReference node, Void context) {
        return resolvePlan.apply(node).accept(this, context);
    }
}
