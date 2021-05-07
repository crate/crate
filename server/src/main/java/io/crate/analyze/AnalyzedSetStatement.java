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
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.SetStatement;

import java.util.List;
import java.util.function.Consumer;

public class AnalyzedSetStatement implements AnalyzedStatement {

    private final List<Assignment<Symbol>> settings;
    private final SetStatement.Scope scope;
    private final boolean persistent;

    AnalyzedSetStatement(SetStatement.Scope scope, List<Assignment<Symbol>> settings, boolean persistent) {
        this.scope = scope;
        this.settings = settings;
        this.persistent = persistent;
    }

    public SetStatement.Scope scope() {
        return scope;
    }

    public List<Assignment<Symbol>> settings() {
        return settings;
    }

    public boolean isPersistent() {
        return persistent;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Assignment<Symbol> symbols : settings) {
            symbols.expressions().forEach(consumer);
        }
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSetStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return SetStatement.Scope.GLOBAL.equals(scope);
    }
}
