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

import io.crate.action.sql.Cursor;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Fetch;
import io.crate.sql.tree.Fetch.ScrollMode;

public class AnalyzedFetch implements AnalyzedStatement {

    private final Fetch fetch;
    private final Cursor cursor;

    public AnalyzedFetch(Fetch fetch, Cursor cursor) {
        this.fetch = fetch;
        this.cursor = cursor;
    }

    public Cursor cursor() {
        return cursor;
    }

    public ScrollMode scrollMode() {
        return fetch.scrollMode();
    }

    public long count() {
        return fetch.count();
    }

    @Override
    public List<Symbol> outputs() {
        return cursor.outputs();
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitFetch(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }
}
