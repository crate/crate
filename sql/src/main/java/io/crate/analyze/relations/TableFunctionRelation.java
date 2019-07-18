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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.tree.QualifiedName;

import java.util.function.Consumer;

public class TableFunctionRelation extends TableRelation {

    private final TableFunctionImplementation functionImplementation;
    private final Function function;

    public TableFunctionRelation(TableInfo tableInfo,
                                 TableFunctionImplementation functionImplementation,
                                 Function function,
                                 QualifiedName qualifiedName) {
        super(tableInfo, qualifiedName);
        this.functionImplementation = functionImplementation;
        this.function = function;
    }

    public Function function() {
        return function;
    }

    public TableFunctionImplementation functionImplementation() {
        return functionImplementation;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunctionRelation(this, context);
    }

    @Override
    public Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation == Operation.READ) {
            return getField(path);
        }
        throw new UnsupportedOperationException("Table functions don't support write operations");
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol argument : function.arguments()) {
            consumer.accept(argument);
        }
    }
}
