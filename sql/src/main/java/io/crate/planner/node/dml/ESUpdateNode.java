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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.operator.Input;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

import javax.annotation.Nullable;
import java.util.*;

public class ESUpdateNode extends DMLPlanNode {

    private final String index;
    private final Map<String, Object> updateDoc;
    private final String[] columns;
    private final WhereClause whereClause;
    private final String[] primaryKeyValues;
    private final Optional<Long> version;


    public ESUpdateNode(String index,
                        Map<Reference, Symbol> assignments,
                        WhereClause whereClause,
                        Optional<Long> version,
                        @Nullable List<Literal> primaryKeyValues) {
        this.index = index;
        if (primaryKeyValues == null) {
            this.primaryKeyValues = new String[0];
        } else {
            assert primaryKeyValues.size() <= 1 : "compound primary keys not supported";
            List<String> pkList = new ArrayList<>();
            for (Literal pkLiteral : primaryKeyValues) {
                if (pkLiteral instanceof SetLiteral) {
                    for (Object setLiteralItem : (Set<?>)pkLiteral.value()) {
                        pkList.add(setLiteralItem.toString());
                    }
                } else {
                    pkList.add(pkLiteral.valueAsString());
                }
            }
            this.primaryKeyValues = pkList.toArray(new String[pkList.size()]);
        }
        this.version = version;
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

    public String[] primaryKeyValues() {
        return primaryKeyValues;
    }

    public String[] columns() {
        return columns;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public Optional<Long> version() {
        return version;
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
}
