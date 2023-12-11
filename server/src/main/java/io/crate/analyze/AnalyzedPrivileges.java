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

import io.crate.role.Privilege;
import io.crate.expression.symbol.Symbol;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class AnalyzedPrivileges implements DCLStatement {

    private final List<String> userNames;
    private final Set<Privilege> privileges;

    AnalyzedPrivileges(List<String> userNames, Set<Privilege> privileges) {
        this.userNames = userNames;
        this.privileges = privileges;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitPrivilegesStatement(this, context);
    }

    public List<String> userNames() {
        return userNames;
    }

    public Set<Privilege> privileges() {
        return privileges;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }
}
