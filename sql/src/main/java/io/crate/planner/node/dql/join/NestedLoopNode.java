/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql.join;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.core.collections.nested.Nested;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class NestedLoopNode extends AbstractDQLPlanNode implements Nested<PlanNode> {

    private Optional<UUID> jobId = Optional.absent();

    private final List<Projection> projections;

    private final DQLPlanNode left;
    private final DQLPlanNode right;
    private final List<PlanNode> children;

    public NestedLoopNode(String id,
                          DQLPlanNode left,
                          DQLPlanNode right,
                          List<Projection> projections) {
        super(id);
        this.left = left;
        this.right = right;
        this.children = ImmutableList.<PlanNode>of(left, right);
        this.projections = projections;
    }

    /**
     * TODO: put this in superclass once we can break serialization format
     */
    public Optional<UUID> jobId() {
        return jobId;
    }

    public void jobId(@Nullable UUID jobId) {
        this.jobId = Optional.fromNullable(jobId);
    }


    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of(); // TODO: localNode?
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopNode(this, context);
    }

    public DQLPlanNode left() {
        return left;
    }

    public DQLPlanNode right() {
        return right;
    }

    public List<Projection> projections() {
        return projections;
    }


    @Override
    public Collection<PlanNode> children() {
        return children;
    }
}
