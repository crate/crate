/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class AnalyzedUpdateStatement implements AnalyzedStatement {

    private final AbstractTableRelation table;
    private final Map<Reference, Symbol> assignmentByTargetCol;
    private final Symbol query;

    @Nullable
    private final Fields fields;

    @Nullable
    private final List<Symbol> returnValues;

    public AnalyzedUpdateStatement(AbstractTableRelation table,
                                   Map<Reference, Symbol> assignmentByTargetCol,
                                   Symbol query,
                                   @Nullable Fields fields,
                                   @Nullable List<Symbol> returnValues
                                   ) {
        this.table = table;
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.query = query;
        this.fields = fields;
        this.returnValues = returnValues;
    }

    public AbstractTableRelation table() {
        return table;
    }

    public Map<Reference, Symbol> assignmentByTargetCol() {
        return assignmentByTargetCol;
    }

    public Symbol query() {
        return query;
    }

    public List<Field> fields() {
        return fields == null ? null : fields.asList();
    }


    public List<Symbol> returnValues() {
        return returnValues;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAnalyzedUpdateStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        consumer.accept(query);
        for (Symbol sourceExpr : assignmentByTargetCol.values()) {
            consumer.accept(sourceExpr);
        }
        if (returnValues != null) {
            for (Symbol returningSymbol : returnValues) {
                consumer.accept(returningSymbol);
            }
        }
        if (fields != null) {
            for (Field field : fields.asList()) {
                consumer.accept(field);
            }
        }
    }

    @Override
    public boolean isUnboundPlanningSupported() {
        return true;
    }
}
