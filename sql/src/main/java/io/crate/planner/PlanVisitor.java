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

package io.crate.planner;

import io.crate.planner.node.dml.InsertFromSubQuery;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.KillPlan;
import org.elasticsearch.common.Nullable;

public class PlanVisitor<C, R> {

    public R process(Plan plan, @Nullable C context) {
        return plan.accept(this, context);
    }

    protected R visitPlan(Plan plan, C context) {
        return null;
    }

    public R visitIterablePlan(IterablePlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitNoopPlan(NoopPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitGlobalAggregate(GlobalAggregate plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitQueryAndFetch(QueryAndFetch node, C context){
        return visitPlan(node, context);
    }

    public R visitQueryThenFetch(QueryThenFetch node, C context){
        return visitPlan(node, context);
    }

    public R visitNonDistributedGroupBy(NonDistributedGroupBy node, C context) {
        return visitPlan(node, context);
    }

    public R visitUpsert(Upsert node, C context) {
        return visitPlan(node, context);
    }

    public R visitDistributedGroupBy(DistributedGroupBy node, C context) {
        return visitPlan(node, context);
    }

    public R visitInsertByQuery(InsertFromSubQuery node, C context) {
        return visitPlan(node, context);
    }

    public R visitCollectAndMerge(CollectAndMerge plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitCountPlan(CountPlan countPlan, C context) {
        return visitPlan(countPlan, context);
    }

    public R visitKillPlan(KillPlan killPlan, C context) {
        return visitPlan(killPlan, context);
    }

    public R visitNestedLoop(NestedLoop nestedLoop, C context) {
        return visitPlan(nestedLoop, context);
    }
}
