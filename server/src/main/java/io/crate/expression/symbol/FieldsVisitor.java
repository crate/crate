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

package io.crate.expression.symbol;

import java.util.function.Consumer;

public final class FieldsVisitor extends DefaultTraversalSymbolVisitor<Consumer<? super ScopedSymbol>, Void> {

    private static final FieldsVisitor FIELDS_VISITOR = new FieldsVisitor();

    private FieldsVisitor() {
        super();
    }

    @Override
    public Void visitField(ScopedSymbol field, Consumer<? super ScopedSymbol> context) {
        context.accept(field);
        return null;
    }

    public static void visitFields(Symbol symbolTree, Consumer<? super ScopedSymbol> consumer) {
        symbolTree.accept(FIELDS_VISITOR, consumer);
    }
}
