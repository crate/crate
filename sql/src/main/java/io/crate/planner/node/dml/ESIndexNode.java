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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;

public class ESIndexNode extends DMLPlanNode {

    private String index;

    private List<Reference> columns;
    private List<List<Symbol>> valuesLists;
    private int[] primaryKeyIndices;

    public ESIndexNode(String index,
                       List<Reference> columns,
                       List<List<Symbol>> valuesLists,
                       @Nullable int[] primaryKeyIndices) {
        Preconditions.checkNotNull(index, "index is null");
        this.index = index;
        this.columns = Objects.firstNonNull(columns, ImmutableList.<Reference>of());
        this.valuesLists = Objects.firstNonNull(valuesLists, ImmutableList.<List<Symbol>>of());
        this.primaryKeyIndices = Objects.firstNonNull(primaryKeyIndices, new int[0]);
    }

    public String index() {
        return index;
    }

    public List<Reference> columns() {
        return columns;
    }

    public List<List<Symbol>> valuesLists() {
        return valuesLists;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESIndexNode(this, context);
    }

    public int[] primaryKeyIndices() {
        return primaryKeyIndices;
    }

    public boolean hasPrimaryKey() {
        return primaryKeyIndices.length > 0;
    }
}
