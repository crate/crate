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

import io.crate.planner.node.ddl.DDLPlanNode;
import io.crate.planner.node.ddl.ESCreateIndexNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.node.dql.ESSearchNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.common.Nullable;

public class PlanVisitor<C, R> {

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

    public R visitESSearchNode(ESSearchNode node, C context) {
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

    public R visitESIndexNode(ESIndexNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESUpdateNode(ESUpdateNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitCopyNode(CopyNode copyNode, C context) {
        return visitPlanNode(copyNode, context);
    }

    public R visitESCreateIndexNode(ESCreateIndexNode esCreateTableNode, C context) {
        return visitDDLPlanNode(esCreateTableNode, context);
    }

    private R visitDDLPlanNode(DDLPlanNode node, C context) {
        return visitPlanNode(node, context);
    }

    public R visitESDeleteIndexNode(ESDeleteIndexNode node, C context) {
        return visitPlanNode(node, context);
    }
}
