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

import io.crate.planner.node.dcl.GenericDCLPlan;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dml.LegacyUpsertById;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.node.management.ShowCreateTablePlan;
import io.crate.planner.statement.SetSessionPlan;
import org.elasticsearch.common.Nullable;

public class ExecutionPlanVisitor<C, R> {

    public R process(ExecutionPlan executionPlan, @Nullable C context) {
        return executionPlan.accept(this, context);
    }

    protected R visitPlan(ExecutionPlan executionPlan, C context) {
        return null;
    }

    public R visitNoopPlan(NoopPlan plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitSetSessionPlan(SetSessionPlan plan, C context) {
        return visitPlan(plan, context);
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

    public R visitShowCreateTable(ShowCreateTablePlan showCreateTablePlan, C context) {
        return visitPlan(showCreateTablePlan, context);
    }

    public R visitGenericDDLPLan(GenericDDLPlan genericDDLPlan, C context) {
        return visitPlan(genericDDLPlan, context);
    }

    public R visitGenericDCLPlan(GenericDCLPlan genericDCLPlan, C context) {
        return visitPlan(genericDCLPlan, context);
    }

    public R visitNestedLoop(NestedLoop plan, C context) {
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

    public R visitDeleteById(DeleteById plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitLegacyUpsertById(LegacyUpsertById plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitDeletePartitions(DeletePartitions plan, C context) {
        return visitPlan(plan, context);
    }

    public R visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, C context) {
        return visitPlan(multiPhasePlan, context);
    }

    public R visitMerge(Merge merge, C context) {
        return visitPlan(merge, context);
    }

    public R visitDeleteAllPartitions(DeleteAllPartitions deleteAllPartitions, C context) {
        return visitPlan(deleteAllPartitions, context);
    }

    public R visitUpdateById(UpdateById updateById, C context) {
        return visitPlan(updateById, context);
    }
}
