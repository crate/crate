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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class NestedLoopNode extends AbstractDQLPlanNode {

    private final DQLPlanNode left;
    private final DQLPlanNode right;
    private boolean leftOuterLoop = true;
    private final @Nullable Integer limit;
    private final int offset;

    public static class Builder {
        private DQLPlanNode left;
        private DQLPlanNode right;
        private Integer limit = null;
        private int offset;
        private boolean leftOuterLoop = true;
        private List<DataType> outputTypes = ImmutableList.of();
        private List<Projection> projections = ImmutableList.of();

        public NestedLoopNode build() {
            Preconditions.checkNotNull(left, "left planNode is null");
            Preconditions.checkNotNull(right, "right planNode is null");
            NestedLoopNode node = new NestedLoopNode(
                    left, right, leftOuterLoop, limit, offset);
            node.outputTypes(outputTypes);
            node.projections(projections);
            return node;
        }

        public Builder left(DQLPlanNode left) {
            this.left = left;
            return this;
        }

        public Builder right(DQLPlanNode right) {
            this.right = right;
            return this;
        }

        public Builder limit(@Nullable Integer limit) {
            this.limit = limit;
            return this;
        }

        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder leftOuterLoop(boolean leftOuterLoop) {
            this.leftOuterLoop = leftOuterLoop;
            return this;
        }

        public Builder outputTypes(List<DataType> outputTypes) {
            this.outputTypes = outputTypes;
            return this;
        }

        public Builder projections(List<Projection> projections) {
            this.projections = projections;
            return this;
        }
    }

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
     */
    private NestedLoopNode(DQLPlanNode left,
                           DQLPlanNode right,
                           boolean leftOuterLoop,
                           @Nullable Integer limit,
                           int offset) {
        super("nestedLoop");
        this.limit = limit;
        this.offset = offset;
        this.leftOuterLoop = leftOuterLoop;
        this.left = left;
        this.right = right;
    }

    public DQLPlanNode left() {
        return left;
    }

    public DQLPlanNode right() {
        return right;
    }

    public DQLPlanNode innerNode() {
        return leftOuterLoop() ? right : left;
    }

    public DQLPlanNode outerNode() {
        return leftOuterLoop() ? left : right;
    }

    public boolean leftOuterLoop() {
        return leftOuterLoop;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    public @Nullable Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
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
                .toString();
    }
}
