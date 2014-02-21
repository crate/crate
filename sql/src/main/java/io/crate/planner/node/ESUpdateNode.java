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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ESUpdateNode extends PlanNode {

    private static final List<DataType> OUTPUT_TYPES = ImmutableList.of(DataType.LONG);

    private final String index;
    private final Map<String, Object> updateDoc;
    private final String[] columns;
    private final WhereClause whereClause;
    private List<Literal> primaryKeyValues;

    public ESUpdateNode(String index,
                        Map<Reference, Symbol> assignments,
                        WhereClause whereClause,
                        @Nullable List<Literal> primaryKeyValues) {
        this.index = index;
        this.primaryKeyValues = Objects.firstNonNull(primaryKeyValues, ImmutableList.<Literal>of());

        updateDoc = new HashMap<>(assignments.size());
        for (Map.Entry<Reference, Symbol> entry: assignments.entrySet()) {
            Object value;
            if (entry.getValue().symbolType() == SymbolType.STRING_LITERAL) {
                value = ((StringLiteral)entry.getValue()).valueAsString();
            } else {
                value = ((Input)entry.getValue()).value();
            }
            updateDoc.put(entry.getKey().info().ident().columnIdent().fqn(), value);
        }
        columns = new String[assignments.size()];
        int i = 0;
        for (Reference ref : assignments.keySet()) {
            columns[i++] = ref.info().ident().columnIdent().fqn();
        }

        this.whereClause = whereClause;
    }

    public List<Literal> primaryKeyValues() {
        return primaryKeyValues;
    }

    public String[] columns() {
        return columns;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESUpdateNode(this, context);
    }

    public String index() {
        return index;
    }

    public Map<String, Object> updateDoc() {
        return updateDoc;
    }

    @Override
    public List<DataType> outputTypes() {
        return OUTPUT_TYPES;
    }
}
