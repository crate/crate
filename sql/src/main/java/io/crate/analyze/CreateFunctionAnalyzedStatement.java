/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.analyze;

import io.crate.sql.tree.Expression;
import io.crate.types.DataType;

import java.util.List;
import java.util.Set;

public class CreateFunctionAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    private final String name;
    private final boolean replace;
    private final List<FunctionArgumentDefinition> arguments;
    private final Set<String> options;
    private final DataType returnType;
    private final Expression language;
    private final Expression body;

    public CreateFunctionAnalyzedStatement(String name,
                                           boolean replace,
                                           List<FunctionArgumentDefinition> arguments,
                                           DataType returnType,
                                           Set<String> options,
                                           Expression language,
                                           Expression body) {
        this.name = name;
        this.replace = replace;
        this.arguments = arguments;
        this.options = options;
        this.returnType = returnType;
        this.language = language;
        this.body = body;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateFunctionStatement(this, context);
    }

    public String name() {
        return name;
    }

    public boolean replace() {
        return replace;
    }

    public Set<String> options() {
        return options;
    }

    public DataType returnType() {
        return returnType;
    }

    public Expression language() {
        return language;
    }

    public Expression body() {
        return body;
    }

    public List<FunctionArgumentDefinition> arguments() {
        return arguments;
    }
}
