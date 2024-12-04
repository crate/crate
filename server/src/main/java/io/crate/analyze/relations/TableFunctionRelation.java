/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.scalar.SubscriptFunctions;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import io.crate.types.RowType;

public class TableFunctionRelation implements AnalyzedRelation, FieldResolver {

    private final TableFunctionImplementation<?> functionImplementation;
    private final Function function;
    private final List<Reference> outputs;
    private final RelationName relationName;

    public TableFunctionRelation(TableFunctionImplementation<?> functionImplementation, Function function) {
        this.functionImplementation = functionImplementation;
        this.function = function;
        RowType rowType = functionImplementation.returnType();
        this.outputs = new ArrayList<>(rowType.numElements());
        int idx = 0;
        FunctionName functionName = function.signature().getName();
        this.relationName = new RelationName(functionName.schema(), functionName.name());
        for (int i = 0; i < rowType.numElements(); i++) {
            DataType<?> type = rowType.getFieldType(i);
            String fieldName = rowType.getFieldName(i);
            var ref = new SimpleReference(new ReferenceIdent(relationName, fieldName), RowGranularity.DOC, type, idx, null);
            outputs.add(ref);
            idx++;
        }
    }

    public Function function() {
        return function;
    }

    public TableFunctionImplementation<?> functionImplementation() {
        return functionImplementation;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunctionRelation(this, context);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        for (Symbol output : outputs) {
            ColumnIdent outputColumn = output.toColumn();
            if (column.equals(outputColumn)) {
                return output;
            }
        }
        ColumnIdent rootColumn = column.getRoot();
        for (Symbol output : outputs) {
            ColumnIdent outputRoot = output.toColumn().getRoot();
            if (output.valueType().id() == ObjectType.ID && rootColumn.equals(outputRoot)) {
                return SubscriptFunctions.makeObjectSubscript(output, column);
            }
        }
        throw ColumnUnknownException.ofTableFunctionRelation("Column " + column.sqlFqn() + " unknown", relationName);
    }

    @Override
    public RelationName relationName() {
        return relationName;
    }

    @NotNull
    @Override
    public List<Symbol> outputs() {
        return List.copyOf(outputs);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs) {
            consumer.accept(output);
        }
        for (Symbol argument : function.arguments()) {
            consumer.accept(argument);
        }
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        return getField(field.column(), Operation.READ, true);
    }

    @Override
    public String toString() {
        return function.toString(Style.UNQUALIFIED);
    }
}
