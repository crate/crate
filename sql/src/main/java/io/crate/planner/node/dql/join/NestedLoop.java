/*
* Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
* license agreements. See the NOTICE file distributed with this work for
* additional information regarding copyright ownership. Crate licenses
* this file to you under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License. You may
* obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*
* However, if you have executed another commercial license agreement
* with Crate these terms will supersede the license and you may use the
* software solely pursuant to the terms of the relevant commercial agreement.
*/
package io.crate.planner.node.dql.join;

import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.Nullable;

import java.util.UUID;

/**
 * Plan that will be executed with the awesome nested loop algorithm
 * performing CROSS JOINs
 *
 * This Plan makes a lot of assumptions:
 *
 * <ul>
 * <li> limit and offset are already pushed down to left and right plan nodes
 * <li> where clause is already splitted to left and right plan nodes
 * <li> order by symbols are already splitted, too
 * <li> if the first order by symbol in the whole statement is from the left node,
 *      set <code>leftOuterLoop</code> to true, otherwise to false
 *
 * </ul>
 *
 * Properties:
 *
 * <ul>
 * <li> the resulting outputs from the join operations are the same, no matter if
 *      <code>leftOuterLoop</code> is true or not - so the projections added,
 *      can assume the same order of symbols, first symbols from left, then from right.
 *      If sth. else is selected a projection has to reorder those.
 *
 */
public class NestedLoop extends PlanAndPlannedAnalyzedRelation {


    private final PlannedAnalyzedRelation left;
    private final PlannedAnalyzedRelation right;
    private final NestedLoopPhase nestedLoopPhase;
    private final UUID jobId;

    @Nullable
    private MergePhase localMergePhase;

    private boolean leftOuterLoop = true;

    /**
     * create a new NestedLoop
     *
     * @param leftOuterLoop if true, indicating that we have to iterate the left
     * side in the outer loop, the right in the inner.
     * Resulting in rows like:
     *
     * a | 1
     * a | 2
     * a | 3
     * b | 1
     * b | 2
     * b | 3
     *
     * This is the case if the left relation is referenced
     * by the first order by symbol references. E.g.
     * for <code>ORDER BY left.a, right.b</code>
     * If false, the nested loop is executed the other way around.
     * With the following results:
     *
     * a | 1
     * b | 1
     * a | 2
     * b | 2
     * a | 3
     * b | 3
     */
    public NestedLoop(UUID jobId,
                      PlannedAnalyzedRelation left,
                      PlannedAnalyzedRelation right,
                      NestedLoopPhase nestedLoopPhase,
                      boolean leftOuterLoop,
                      @Nullable MergePhase localMergePhase) {
        this.jobId = jobId;
        this.leftOuterLoop = leftOuterLoop;
        this.left = left;
        this.right = right;
        this.nestedLoopPhase = nestedLoopPhase;
        this.localMergePhase = localMergePhase;
    }

    public PlannedAnalyzedRelation left() {
        return left;
    }

    public PlannedAnalyzedRelation right() {
        return right;
    }

    public PlannedAnalyzedRelation inner() {
        return leftOuterLoop() ? right : left;
    }

    public PlannedAnalyzedRelation outer() {
        return leftOuterLoop() ? left : right;
    }

    public boolean leftOuterLoop() {
        return leftOuterLoop;
    }

    public NestedLoopPhase nestedLoopPhase() {
        return nestedLoopPhase;
    }

    @Nullable
    public MergePhase localMergePhase() {
        return localMergePhase;
    }

    @Override
    public void addProjection(Projection projection) {
        if (localMergePhase != null) {
            localMergePhase.projections().add(projection);
        } else {
            nestedLoopPhase.projections().add(projection);
        }
    }

    @Override
    public boolean resultIsDistributed() {
        return false;
    }

    @Override
    public DQLPlanNode resultNode() {
        if (localMergePhase != null) {
            return localMergePhase;
        } else if (nestedLoopPhase != null) {
            return nestedLoopPhase;
        } else {
            return outer().resultNode();
        }
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoop(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}
