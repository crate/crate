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

package io.crate.planner.operators;

import io.crate.planner.optimizer.iterative.GroupReference;

public class LogicalPlanVisitor<C, R> {

    public R visitPlan(LogicalPlan logicalPlan, C context) {
        return null;
    }

    public R visitMultiPhase(MultiPhase logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitLookupJoin(LookupJoin logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitCollect(Collect logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitCount(Count logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitGet(Get logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitEval(Eval logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitFilter(Filter logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitGroupHashAggregate(GroupHashAggregate logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitHashAggregate(HashAggregate logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitInsert(Insert logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitInsert(InsertFromValues logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitJoinPlan(JoinPlan logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitNestedLoopJoin(NestedLoopJoin logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitHashJoin(HashJoin logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitLimit(Limit logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitOrder(Order logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitRootRelationBoundary(RootRelationBoundary logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitUnion(Union logicalPlan, C context) {
        return visitPlan(logicalPlan, context);
    }

    public R visitProjectSet(ProjectSet projectSet, C context) {
        return visitPlan(projectSet, context);
    }

    public R visitWindowAgg(WindowAgg windowAgg, C context) {
        return visitPlan(windowAgg, context);
    }

    public R visitLimitDistinct(LimitDistinct limitDistinct, C context) {
        return visitPlan(limitDistinct, context);
    }

    public R visitTableFunction(TableFunction tableFunction, C context) {
        return visitPlan(tableFunction, context);
    }

    public R visitRename(Rename rename, C context) {
        return visitPlan(rename, context);
    }

    public R visitFetch(Fetch fetch, C context) {
        return visitPlan(fetch, context);
    }

    public R visitCorrelatedJoin(CorrelatedJoin apply, C context) {
        return visitPlan(apply, context);
    }

    public R visitGroupReference(GroupReference apply, C context) {
        return visitPlan(apply, context);
    }

    public R visitForeignCollect(ForeignCollect foreignCollect, C context) {
        return visitPlan(foreignCollect, context);
    }
}
