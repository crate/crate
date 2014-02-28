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

package io.crate.planner.node;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;

import java.util.Set;

public class ESDeleteByQueryNode extends DQLPlanNode {

    private final Set<String> indices;
    private final WhereClause whereClause;

    public ESDeleteByQueryNode(Set<String> indices, WhereClause whereClause) {
        assert whereClause != null;
        this.indices = indices;
        this.whereClause = whereClause;
    }

    public Set<String> indices() {
        return indices;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public Set<String> executionNodes() {
        // always runs local (aka handler) since it uses its own routing internally
        return ImmutableSet.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESDeleteByQueryNode(this, context);
    }
}
