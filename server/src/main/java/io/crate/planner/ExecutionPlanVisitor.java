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

package io.crate.planner;

import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;
import org.jspecify.annotations.Nullable;

public class ExecutionPlanVisitor<C, R> {

    public R process(ExecutionPlan executionPlan, @Nullable C context) {
        return executionPlan.accept(this, context);
    }

    protected R visitPlan(ExecutionPlan executionPlan, C context) {
        return null;
    }

    public R visitCollect(Collect plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitQueryThenFetch(QueryThenFetch plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitCountPlan(CountPlan countPlan, C context) {
        return visitPlan(countPlan, context);
    }

    public R visitJoin(Join plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitMerge(Merge merge, C context) {
        return visitPlan(merge, context);
    }

    public R visitUnionPlan(UnionExecutionPlan unionExecutionPlan, C context) {
        return visitPlan(unionExecutionPlan, context);
    }
}
