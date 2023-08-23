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

package io.crate.planner.operators;

import java.util.HashSet;
import java.util.Set;

import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public final class ColumnIndentExtractor {

    private ColumnIndentExtractor() {}

    private static final Visitor VISITOR = new Visitor();

    public static Set<ColumnIdent> extract(Symbol symbol) {
        Context ctx = new Context();
        symbol.accept(VISITOR, ctx);
        return ctx.columnIdents;
    }

    private static class Context {
        Set<ColumnIdent> columnIdents = new HashSet<>();
    }

    private static class Visitor extends DefaultTraversalSymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function symbol, Context context) {
            for (Symbol arg : symbol.arguments()) {
                arg.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitReference(Reference reference, Context context) {
            context.columnIdents.add(reference.column());
            return null;
        }

        @Override
        public Void visitField(ScopedSymbol field, Context context) {
            context.columnIdents.add(field.column());
            return null;
        }
    }

}
