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

import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.symbol.Field;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Plan Node that will be executed with the awesome nested loop algorithm
 * performing CROSS JOINs
 *
 * This Node makes a lot of assumptions:
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
public class NestedLoopNode extends AbstractDQLPlanNode implements PlannedAnalyzedRelation {


    private final PlanNode left;
    private final PlanNode right;
    private boolean leftOuterLoop = true;
    private final int limit;
    private final int offset;
    private final boolean sorted;

    /**
     * create a new NestedLoopNode
     *
     * @param limit the maximum number of rows to return
     * @param offset the number of rows to skip
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
     *
     * @param sorted true if the statement this NestedLoop belongs to is sorted
     *               i.e. has an order by clause. In case of subselects with a NestedLoop
     *               the value of <code>sorted</code> depends on the ordering of
     *               the subselect not the whole statement.
     *
     */
    public NestedLoopNode(PlanNode left,
                           PlanNode right,
                           boolean leftOuterLoop,
                           int limit,
                           int offset,
                           boolean sorted) {
        super("nestedLoop");
        this.limit = limit;
        this.offset = offset;
        this.leftOuterLoop = leftOuterLoop;
        this.left = left;
        this.right = right;
        this.sorted = sorted;
        // fallback
        this.outputTypes(Lists.newArrayList(FluentIterable.from(left.outputTypes()).append(right.outputTypes())));
    }

    public PlanNode left() {
        return left;
    }

    public PlanNode right() {
        return right;
    }

    public PlanNode inner() {
        return leftOuterLoop() ? right : left;
    }

    public PlanNode outer() {
        return leftOuterLoop() ? left : right;
    }

    public boolean leftOuterLoop() {
        return leftOuterLoop;
    }

    public boolean sorted() {
        return sorted;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("NestedLoopNode not serializable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("NestedLoopNode not serializable");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id())
                .add("projections", projections)
                .add("outputTypes", outputTypes)
                .add("left", left)
                .add("right", right)
                .add("offset", offset())
                .add("limit", limit())
                .add("leftOuterLoop", leftOuterLoop)
                .add("sorted", sorted)
                .toString();
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        return null;
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        return null;
    }

    @Override
    public List<Field> fields() {
        return null;
    }

    @Override
    public Plan plan() {
        return new IterablePlan(this);
    }
}
