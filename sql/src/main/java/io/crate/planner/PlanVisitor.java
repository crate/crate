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

import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDelete;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.GenericShowPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.statement.SetSessionPlan;
import org.elasticsearch.common.Nullable;

public class PlanVisitor<C, R> {

    public R process(Plan plan, @Nullable C context) {
        return plan.accept(this, context);
    }

    protected R visitPlan(Plan plan, C context) {
        return null;
    }

    public R visitNoopPlan(NoopPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitSetSessionPlan(SetSessionPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitUpsert(Upsert node, C context) {
        return visitPlan(node, context);
    }

    public R visitDistributedGroupBy(DistributedGroupBy node, C context) {
        return visitPlan(node, context);
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

    public R visitKillPlan(KillPlan killPlan, C context) {
        return visitPlan(killPlan, context);
    }

    public R visitExplainPlan(ExplainPlan explainPlan, C context) {
        return visitPlan(explainPlan, context);
    }

    public R visitGenericShowPlan(GenericShowPlan genericShowPlan, C context) {
        return visitPlan(genericShowPlan, context);
    }

    public R visitGenericDDLPLan(GenericDDLPlan genericDDLPlan, C context) {
        return visitPlan(genericDDLPlan, context);
    }

    public R visitNestedLoop(NestedLoop plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitUnion(UnionPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitGetPlan(ESGet plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitDropTablePlan(DropTablePlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitESClusterUpdateSettingsPlan(ESClusterUpdateSettingsPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitCreateAnalyzerPlan(CreateAnalyzerPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitESDelete(ESDelete plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitUpsertById(UpsertById plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitESDeletePartition(ESDeletePartition plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, C context) {
        return visitPlan(multiPhasePlan, context);
    }

    public R visitMerge(Merge merge, C context) {
        return visitPlan(merge, context);
    }
}
