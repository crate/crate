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

package io.crate.planner.node;

import com.google.common.collect.ImmutableList;
import io.crate.planner.Planner;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import java.util.List;

public class ProjectedNode implements PlanNode {

    private final PlanNode node;
    private final Projection projection;

    public ProjectedNode(PlanNode node, Projection projection) {
        this.node = node;
        this.projection = projection;
    }

    public PlanNode node() {
        return node;
    }

    public Projection projection() {
        return projection;
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection not supported");
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitProjectedNode(this, context);
    }

    @Override
    public List<DataType> outputTypes() {
        return Planner.extractDataTypes(ImmutableList.of(projection), node.outputTypes());
    }

    @Override
    public void outputTypes(List<DataType> outputTypes) {
        throw new UnsupportedOperationException("outputTypes cannot be modified on a ProjectedNode");
    }
}
