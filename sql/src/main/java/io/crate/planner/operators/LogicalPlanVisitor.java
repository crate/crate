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

package io.crate.planner.operators;

public class LogicalPlanVisitor<C, R> {

    protected R visitPlan(LogicalPlan logicalPlan, C context) {
        return null;
    }

    public R visitMultiPhase(MultiPhase logicalPlan, C context) {
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

    public R visitRelationBoundary(RelationBoundary logicalPlan, C context) {
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

    public R visitTopNDistinct(TopNDistinct topNDistinct, C context) {
        return visitPlan(topNDistinct, context);
    }

    public R visitTableFunction(TableFunction tableFunction, C context) {
        return visitPlan(tableFunction, context);
    }
}
