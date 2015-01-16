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

package io.crate.planner.node.dml;

import com.google.common.base.Optional;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Symbol;

import java.util.Map;

public class UpdateByIdExecutionNode extends DMLPlanNode {

    private final String index;
    private final String id;
    private final String routing;
    private final Map<String, Symbol> assignments;
    private final Optional<Long> version;

    public UpdateByIdExecutionNode(String index,
                                   String id,
                                   String routing,
                                   Map<String, Symbol> assignments,
                                   Optional<Long> version) {
        this.index = index;
        this.id = id;
        this.routing = routing;
        this.assignments = assignments;
        this.version = version;
    }

    public String index() {
        return index;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return routing;
    }

    public Map<String, Symbol> assignments() {
        return assignments;
    }

    public Optional<Long> version() {
        return version;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitUpdateByIdExecutionNode(this, context);
    }
}
