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

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;

import java.util.List;
import java.util.function.Consumer;

public class AnalyzedDropFunction implements AnalyzedStatement {

    private final String schema;
    private final String name;
    private final boolean ifExists;
    private final List<DataType<?>> argumentTypes;

    AnalyzedDropFunction(String schema, String name, boolean ifExists, List<DataType<?>> argumentTypes) {
        this.schema = schema;
        this.name = name;
        this.ifExists = ifExists;
        this.argumentTypes = argumentTypes;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDropFunction(this, context);
    }

    public String name() {
        return name;
    }

    public String schema() {
        return schema;
    }

    public List<DataType<?>> argumentTypes() {
        return argumentTypes;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }
}
