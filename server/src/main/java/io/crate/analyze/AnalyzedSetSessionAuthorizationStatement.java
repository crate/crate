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
import io.crate.sql.tree.SetSessionAuthorizationStatement;

import javax.annotation.Nullable;
import java.util.function.Consumer;

public class AnalyzedSetSessionAuthorizationStatement implements AnalyzedStatement {

    @Nullable
    private final String user;
    private final SetSessionAuthorizationStatement.Scope scope;

    AnalyzedSetSessionAuthorizationStatement(@Nullable String user,
                                             SetSessionAuthorizationStatement.Scope scope) {
        this.user = user;
        this.scope = scope;
    }

    /**
     * user is null for the following statements::
     * <p>
     * SET [ SESSION | LOCAL ] SESSION AUTHORIZATION DEFAULT
     * and
     * RESET SESSION AUTHORIZATION
     */
    @Nullable
    public String user() {
        return user;
    }

    public SetSessionAuthorizationStatement.Scope scope() {
        return scope;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSetSessionAuthorizationStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }
}
