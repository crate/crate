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

package io.crate.planner.node.dql;

import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.node.PlanNode;

import java.util.Arrays;
import java.util.Iterator;

public class CollectAndMerge implements Iterable<PlanNode>, Plan {

    private final CollectNode collectNode;
    private final MergeNode localMergeNode;
    private Iterable<PlanNode> nodes;

    public CollectAndMerge(CollectNode collectNode, MergeNode localMergeNode) {
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
        nodes = Arrays.<PlanNode>asList(collectNode, localMergeNode);
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    public MergeNode localMergeNode() {
        return localMergeNode;
    }

    @Override
    public Iterator<PlanNode> iterator() {
        return nodes.iterator();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollectAndMerge(this, context);
    }
}
