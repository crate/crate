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

package io.crate.analyze.relations;

import io.crate.analyze.Fields;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.RowType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class TableFunctionRelation implements AnalyzedRelation, FieldResolver {

    private final TableFunctionImplementation<?> functionImplementation;
    private final Function function;
    private final QualifiedName qualifiedName;
    private final Fields fields;
    private final List<Symbol> outputs;

    public TableFunctionRelation(TableFunctionImplementation<?> functionImplementation,
                                 Function function,
                                 QualifiedName qualifiedName) {
        this.functionImplementation = functionImplementation;
        this.function = function;
        this.qualifiedName = qualifiedName;
        RowType rowType = functionImplementation.returnType();
        this.fields = new Fields(rowType.numElements());
        this.outputs = new ArrayList<>(rowType.numElements());
        int idx = 0;
        FunctionName functionName = function.info().ident().fqnName();
        var relationName = new RelationName(Objects.requireNonNullElse(functionName.schema(), ""), functionName.name());
        for (int i = 0; i < rowType.numElements(); i++) {
            DataType<?> type = rowType.getFieldType(i);
            String fieldName = rowType.getFieldName(i);
            var ref = new Reference(new ReferenceIdent(relationName, fieldName), RowGranularity.DOC, type, idx, null);
            outputs.add(ref);
            fields.add(new Field(this, new ColumnIdent(fieldName), ref));
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
    public Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation == Operation.READ) {
            return fields.get(path);
        }
        throw new UnsupportedOperationException("Table functions don't support write operations");
    }

    @Nonnull
    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
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

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Nullable
    @Override
    public Symbol resolveField(Field field) {
        Field resolvedField = fields.get(field.path());
        return resolvedField == null ? null : resolvedField.pointer();
    }

    @Override
    public String toString() {
        return SymbolPrinter.printUnqualified(function);
    }
}
