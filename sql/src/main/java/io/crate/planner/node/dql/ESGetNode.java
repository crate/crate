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

package io.crate.planner.node.dql;

import com.google.common.base.Objects;
import io.crate.planner.node.PlanVisitor;

import java.util.Arrays;
import java.util.List;


public class ESGetNode extends ESDQLPlanNode implements DQLPlanNode {

    private final String index;
    private final List<String> ids;
    private final List<String> routingValues;

    public ESGetNode(String index, List<String> ids, List<String> routingValues) {
        this.index = index;
        this.ids = ids;
        this.routingValues = routingValues;
    }

    public ESGetNode(String index, String id, String routingValue) {
        this.index = index;
        this.ids = Arrays.asList(id);
        this.routingValues = Arrays.asList(routingValue);
    }

    public String index() {
        return index;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESGetNode(this, context);
    }

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("index", index)
                .add("ids", ids)
                .add("outputs", outputs)
                .toString();
    }
}
