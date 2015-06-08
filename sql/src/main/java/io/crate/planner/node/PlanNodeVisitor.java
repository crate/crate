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

import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.common.Nullable;

public class PlanNodeVisitor<C, R> {

    public R process(PlanNode node, @Nullable C context) {
        return node.accept(this, context);
    }

    protected R visitPlanNode(PlanNode node, C context) {
        return null;
    }

    public R visitMergeNode(MergeNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitCollectNode(CollectNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESGetNode(ESGetNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESDeleteByQueryNode(ESDeleteByQueryNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESDeleteNode(ESDeleteNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitSymbolBasedUpsertByIdNode(SymbolBasedUpsertByIdNode node, C context) {
        return visitPlanNode(node, context);
    }

    private R visitDDLPlanNode(DDLPlanNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESDeletePartitionNode(ESDeletePartitionNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, C context) {
        return visitDDLPlanNode(node, context);
    }

    public R visitESCreateTemplateNode(ESCreateTemplateNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitDropTableNode(DropTableNode node, C context) {
        return visitDDLPlanNode(node, context);
    }

    public R visitCreateTableNode(CreateTableNode node, C context) {
        return visitDDLPlanNode(node, context);
    }

    public R visitGenericDDLNode(GenericDDLNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitNestedLoopNode(NestedLoopNode node, C context) {
        return visitPlanNode(node, context);
    }

}
