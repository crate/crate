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

package io.crate.planner.node.dml;

import io.crate.planner.node.PlanVisitor;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * plan node for 1 or more documents to index via ESIndexTask
 * for a single index
 */
public class ESIndexNode extends DMLPlanNode {

    private String index;

    private List<Map<String, Object>> sourceMaps;
    private List<String> ids;
    private List<String> routingValues;

    public ESIndexNode(String index,
                       List<Map<String, Object>> sourceMaps,
                       List<String> ids,
                       @Nullable List<String> routingValues) {
        assert index != null : "index is null";
        this.index = index;
        this.sourceMaps = sourceMaps;
        this.ids = ids;
        this.routingValues = routingValues;
    }

    public String index() {
        return index;
    }

    public List<Map<String, Object>> sourceMaps() {
        return sourceMaps;
    }

    public List<String> ids() {
        return ids;
    }

    @Nullable
    public List<String> routingValues() {
        return routingValues;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESIndexNode(this, context);
    }

}
