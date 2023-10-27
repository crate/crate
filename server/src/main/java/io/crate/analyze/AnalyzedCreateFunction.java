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

import java.util.List;
import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;

public class AnalyzedCreateFunction implements DDLStatement {

    private final String name;
    private final String schema;
    private final boolean replace;
    private final List<FunctionArgumentDefinition> arguments;
    private final DataType<?> returnType;
    private final Symbol language;
    private final Symbol definition;

    AnalyzedCreateFunction(String schema,
                           String name,
                           boolean replace,
                           List<FunctionArgumentDefinition> arguments,
                           DataType<?> returnType,
                           Symbol language,
                           Symbol definition) {
        this.name = name;
        this.schema = schema;
        this.replace = replace;
        this.arguments = arguments;
        this.returnType = returnType;
        this.language = language;
        this.definition = definition;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateFunction(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        consumer.accept(language);
        consumer.accept(definition);
    }

    public String name() {
        return name;
    }

    public String schema() {
        return schema;
    }

    public boolean replace() {
        return replace;
    }

    public DataType<?> returnType() {
        return returnType;
    }

    public Symbol language() {
        return language;
    }

    public Symbol definition() {
        return definition;
    }

    public List<FunctionArgumentDefinition> arguments() {
        return arguments;
    }
}
