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

package io.crate.planner.node;

import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.elasticsearch.common.Nullable;

public class PlanVisitor<C, R> {

    public R process(PlanNode node, @Nullable C context) {
        return node.accept(this, context);
    }

    protected R visitPlanNode(PlanNode node, C context) {
        return null;
    }

    private R visitProjection(Projection projection, C context) {
        // TODO: we should probably add a super interface to projections and plan nodes
        return null;
    }

    public R visitMergeNode(MergeNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitCollectNode(CollectNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESSearchNode(ESSearchNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESGetNode(ESGetNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitAggregationProjection(AggregationProjection projection, C context) {
        return visitProjection(projection, context);
    }

    public R visitGroupProjection(GroupProjection projection, C context) {
        return visitProjection(projection, context);
    }

    public R visitTopNProjection(TopNProjection projection, C context) {
        return visitProjection(projection, context);
    }

}
