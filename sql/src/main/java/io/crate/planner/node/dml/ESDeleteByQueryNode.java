/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node.dml;

import io.crate.analyze.WhereClause;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.ESDQLPlanNode;

import java.util.ArrayList;
import java.util.List;

public class ESDeleteByQueryNode extends DMLPlanNode {

    private final int executionNodeId;
    private final List<String[]> indices;
    private final List<WhereClause> whereClauses;
    private final List<String> routings;

    public ESDeleteByQueryNode(int executionNodeId,
                               List<String[]> indices,
                               List<WhereClause> whereClauses) {
        assert whereClauses.size() > 0;
        this.executionNodeId = executionNodeId;
        this.indices = indices;
        this.whereClauses = whereClauses;
        this.routings = new ArrayList<>(whereClauses.size());
        for (WhereClause whereClause : whereClauses) {
            routings.add(ESDQLPlanNode.noCommaStringRouting(whereClause.clusteredBy()));
        }
    }

    public int executionNodeId() {
        return executionNodeId;
    }

    public List<String[]> indices() {
        return indices;
    }

    public List<String> routings() {
        return routings;
    }

    public List<WhereClause> whereClauses() {
        return whereClauses;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESDeleteByQueryNode(this, context);
    }
}
